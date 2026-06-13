package iot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// MTP — Machine Telemetry Protocol v1. A compact big-endian binary framing for
// PLC/edge relays that cannot speak HTTP, modeled on the Teltonika Codec 8 wire
// shape used by Fleet_IoT (length-prefixed handshake, preamble+length+CRC data
// packet, single-byte/int32 ACK).
//
// Handshake (device → server):  [uint16 len][serial bytes]
//
//	server replies one byte: 0x01 accept, 0x00 reject.
//
// Data packet (device → server):
//
//	[uint32 preamble=0][uint32 dataLen][body][uint32 crc]
//	crc is crc16IBM(body) in the low 16 bits.
//	body = [byte codecID=0x4D 'M'][byte count1][count1 × record][byte count2]
//	count1 must equal count2.
//
// ACK (server → device): [uint32 acceptedRecordCount]
const (
	MTPCodecID    byte = 0x4D // 'M'
	mtpRecordSize      = 39
	mtpMaxData         = 1 << 20 // 1 MiB body cap
)

var (
	ErrBadPreamble = errors.New("mtp: non-zero preamble")
	ErrBadCRC      = errors.New("mtp: crc mismatch")
	ErrBadCodec    = errors.New("mtp: unsupported codec id")
	ErrBadCount    = errors.New("mtp: record count mismatch")
	ErrShortBody   = errors.New("mtp: body shorter than declared records")
)

// MTPRecord is one decoded sample off the wire. Scaled integer fields are
// converted to engineering units by RecordToReading.
type MTPRecord struct {
	Timestamp   time.Time
	State       uint8
	FaultCode   uint16
	SpindleRPM  uint32
	GoodCount   uint32
	RejectCount uint32
	CycleCount  uint32
	TempCentiC  int32  // temperature × 100, °C
	VibMilliMmS uint32 // vibration × 1000, mm/s
	PowerW      uint32 // power, watts
}

// ReadHandshake reads the length-prefixed device serial.
func ReadHandshake(r io.Reader) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", err
	}
	n := binary.BigEndian.Uint16(lenBuf[:])
	if n == 0 || n > 64 {
		return "", fmt.Errorf("mtp: implausible serial length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// WriteHandshakeResponse writes the one-byte accept/reject reply.
func WriteHandshakeResponse(w io.Writer, accept bool) error {
	b := byte(0x00)
	if accept {
		b = 0x01
	}
	_, err := w.Write([]byte{b})
	return err
}

// WriteACK writes the four-byte accepted-record count.
func WriteACK(w io.Writer, count int) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(count))
	_, err := w.Write(buf[:])
	return err
}

// ReadMTPPacket reads and validates one data packet, returning the codec id and
// decoded records. Errors are terminal for the connection.
func ReadMTPPacket(r *bufio.Reader) (byte, []MTPRecord, error) {
	var preamble [4]byte
	if _, err := io.ReadFull(r, preamble[:]); err != nil {
		return 0, nil, err
	}
	if binary.BigEndian.Uint32(preamble[:]) != 0 {
		return 0, nil, ErrBadPreamble
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	dataLen := binary.BigEndian.Uint32(lenBuf[:])
	if dataLen < 3 || dataLen > mtpMaxData {
		return 0, nil, fmt.Errorf("mtp: implausible data length %d", dataLen)
	}
	body := make([]byte, dataLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return 0, nil, err
	}
	if uint16(binary.BigEndian.Uint32(crcBuf[:])) != crc16IBM(body) {
		return 0, nil, ErrBadCRC
	}

	codec := body[0]
	if codec != MTPCodecID {
		return 0, nil, ErrBadCodec
	}
	count1 := int(body[1])
	need := 2 + count1*mtpRecordSize + 1
	if len(body) < need {
		return 0, nil, ErrShortBody
	}
	count2 := int(body[2+count1*mtpRecordSize])
	if count1 != count2 {
		return 0, nil, ErrBadCount
	}

	records := make([]MTPRecord, 0, count1)
	off := 2
	for i := 0; i < count1; i++ {
		rec := body[off : off+mtpRecordSize]
		records = append(records, MTPRecord{
			Timestamp:   time.UnixMilli(int64(binary.BigEndian.Uint64(rec[0:8]))).UTC(),
			State:       rec[8],
			FaultCode:   binary.BigEndian.Uint16(rec[9:11]),
			SpindleRPM:  binary.BigEndian.Uint32(rec[11:15]),
			GoodCount:   binary.BigEndian.Uint32(rec[15:19]),
			RejectCount: binary.BigEndian.Uint32(rec[19:23]),
			CycleCount:  binary.BigEndian.Uint32(rec[23:27]),
			TempCentiC:  int32(binary.BigEndian.Uint32(rec[27:31])),
			VibMilliMmS: binary.BigEndian.Uint32(rec[31:35]),
			PowerW:      binary.BigEndian.Uint32(rec[35:39]),
		})
		off += mtpRecordSize
	}
	return codec, records, nil
}

// RecordToReading converts a wire record into a Reading bound to the device's
// machine, applying scaling and treating fault code 0 as "no fault".
func RecordToReading(rec MTPRecord, device *Device) Reading {
	devID := device.ID
	rpm := float64(rec.SpindleRPM)
	temp := float64(rec.TempCentiC) / 100.0
	vib := float64(rec.VibMilliMmS) / 1000.0
	powerKW := float64(rec.PowerW) / 1000.0
	good := int64(rec.GoodCount)
	reject := int64(rec.RejectCount)
	cycles := int64(rec.CycleCount)

	r := Reading{
		AssetTag:     device.AssetTag,
		DeviceID:     &devID,
		TS:           rec.Timestamp,
		State:        StateForCode(rec.State),
		SpindleRPM:   &rpm,
		TemperatureC: &temp,
		VibrationMmS: &vib,
		PowerKW:      &powerKW,
		GoodCount:    &good,
		RejectCount:  &reject,
		CycleCount:   &cycles,
	}
	if rec.FaultCode != 0 {
		fc := fmt.Sprintf("F%04d", rec.FaultCode)
		r.FaultCode = &fc
	}
	return r
}

// crc16IBM computes CRC-16/IBM (a.k.a. CRC-16/ARC): reflected polynomial 0xA001,
// init 0x0000. Same algorithm Teltonika uses, matched here so MTP framing reuses
// a well-known checksum implementations exist for on the relay side.
func crc16IBM(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}
