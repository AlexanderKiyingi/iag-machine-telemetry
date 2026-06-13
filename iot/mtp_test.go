package iot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

// buildMTPPacket constructs a syntactically valid single-record MTP packet with
// the given values and a correct CRC.
func buildMTPPacket(t *testing.T, rec MTPRecord) []byte {
	t.Helper()
	body := make([]byte, 0, 2+mtpRecordSize+1)
	body = append(body, MTPCodecID)
	body = append(body, 0x01) // count1

	body = appendU64(body, uint64(rec.Timestamp.UnixMilli()))
	body = append(body, rec.State)
	body = appendU16(body, rec.FaultCode)
	body = appendU32(body, rec.SpindleRPM)
	body = appendU32(body, rec.GoodCount)
	body = appendU32(body, rec.RejectCount)
	body = appendU32(body, rec.CycleCount)
	body = appendU32(body, uint32(rec.TempCentiC))
	body = appendU32(body, rec.VibMilliMmS)
	body = appendU32(body, rec.PowerW)

	body = append(body, 0x01) // count2

	out := make([]byte, 0, len(body)+12)
	out = appendU32(out, 0) // preamble
	out = appendU32(out, uint32(len(body)))
	out = append(out, body...)
	out = appendU32(out, uint32(crc16IBM(body)))
	return out
}

func appendU16(b []byte, v uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	return append(b, buf[:]...)
}
func appendU32(b []byte, v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return append(b, buf[:]...)
}
func appendU64(b []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}

func TestReadMTPPacketRoundTrip(t *testing.T) {
	want := MTPRecord{
		Timestamp:   time.Unix(1_700_000_000, 0).UTC(),
		State:       3, // down
		FaultCode:   42,
		SpindleRPM:  12000,
		GoodCount:   980,
		RejectCount: 20,
		CycleCount:  1000,
		TempCentiC:  6450, // 64.50 °C
		VibMilliMmS: 2750, // 2.750 mm/s
		PowerW:      7400,
	}
	raw := buildMTPPacket(t, want)

	r := bufio.NewReader(bytes.NewReader(raw))
	codec, recs, err := ReadMTPPacket(r)
	if err != nil {
		t.Fatalf("ReadMTPPacket: %v", err)
	}
	if codec != MTPCodecID {
		t.Errorf("codec = %#x, want %#x", codec, MTPCodecID)
	}
	if len(recs) != 1 {
		t.Fatalf("len(recs) = %d, want 1", len(recs))
	}
	got := recs[0]
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", got.Timestamp, want.Timestamp)
	}
	if got != want {
		t.Errorf("record = %+v, want %+v", got, want)
	}
}

func TestRecordToReadingScaling(t *testing.T) {
	dev := &Device{ID: 7, MachineID: "MCH-001"}
	rec := MTPRecord{
		Timestamp:   time.Unix(1_700_000_000, 0).UTC(),
		State:       1, // running
		FaultCode:   0,
		SpindleRPM:  9000,
		GoodCount:   500,
		RejectCount: 5,
		CycleCount:  505,
		TempCentiC:  3300, // 33.00 °C
		VibMilliMmS: 1500, // 1.5 mm/s
		PowerW:      4200, // 4.2 kW
	}
	r := RecordToReading(rec, dev)
	if r.MachineID != "MCH-001" || r.DeviceID == nil || *r.DeviceID != 7 {
		t.Fatalf("binding wrong: %+v", r)
	}
	if r.State != StateRunning {
		t.Errorf("State = %q, want running", r.State)
	}
	if r.TemperatureC == nil || *r.TemperatureC != 33.0 {
		t.Errorf("TemperatureC = %v, want 33.0", r.TemperatureC)
	}
	if r.VibrationMmS == nil || *r.VibrationMmS != 1.5 {
		t.Errorf("VibrationMmS = %v, want 1.5", r.VibrationMmS)
	}
	if r.PowerKW == nil || *r.PowerKW != 4.2 {
		t.Errorf("PowerKW = %v, want 4.2", r.PowerKW)
	}
	if r.FaultCode != nil {
		t.Errorf("FaultCode = %v, want nil for code 0", *r.FaultCode)
	}
}

func TestHandshake(t *testing.T) {
	serial := "MCH-EDGE-000123"
	raw := append([]byte{0x00, byte(len(serial))}, []byte(serial)...)
	got, err := ReadHandshake(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("ReadHandshake: %v", err)
	}
	if got != serial {
		t.Errorf("got %q want %q", got, serial)
	}

	var w bytes.Buffer
	if err := WriteHandshakeResponse(&w, true); err != nil {
		t.Fatal(err)
	}
	if w.Len() != 1 || w.Bytes()[0] != 0x01 {
		t.Errorf("accept reply = % x, want 01", w.Bytes())
	}
	w.Reset()
	if err := WriteHandshakeResponse(&w, false); err != nil {
		t.Fatal(err)
	}
	if w.Bytes()[0] != 0x00 {
		t.Errorf("reject reply = % x, want 00", w.Bytes())
	}
}

func TestACK(t *testing.T) {
	var w bytes.Buffer
	if err := WriteACK(&w, 7); err != nil {
		t.Fatal(err)
	}
	if got := w.Bytes(); len(got) != 4 || got[3] != 0x07 || got[0] != 0 {
		t.Errorf("ACK bytes = % x, want 00000007", got)
	}
}

func TestBadPreamble(t *testing.T) {
	raw := bytes.Repeat([]byte{0xFF}, 32)
	_, _, err := ReadMTPPacket(bufio.NewReader(bytes.NewReader(raw)))
	if err == nil {
		t.Fatal("expected error for non-zero preamble")
	}
}

func TestBadCRC(t *testing.T) {
	raw := buildMTPPacket(t, MTPRecord{Timestamp: time.Now().UTC(), State: 1})
	raw[len(raw)-1] ^= 0xFF // corrupt the CRC
	_, _, err := ReadMTPPacket(bufio.NewReader(bytes.NewReader(raw)))
	if err == nil {
		t.Fatal("expected CRC error")
	}
}

func TestCountMismatch(t *testing.T) {
	raw := buildMTPPacket(t, MTPRecord{Timestamp: time.Now().UTC(), State: 1})
	// The trailing count2 byte sits just before the 4-byte CRC.
	body := raw[8 : len(raw)-4]
	body[len(body)-1] = 0x02 // count2 != count1
	// Recompute CRC so the count check (not CRC) is what fails.
	crc := crc16IBM(body)
	binary.BigEndian.PutUint32(raw[len(raw)-4:], uint32(crc))
	_, _, err := ReadMTPPacket(bufio.NewReader(bytes.NewReader(raw)))
	if err != ErrBadCount {
		t.Fatalf("err = %v, want ErrBadCount", err)
	}
}

func TestCRC16IBMKnownVectors(t *testing.T) {
	cases := []struct {
		in   []byte
		want uint16
	}{
		{[]byte{}, 0x0000},
		{[]byte{0x01}, 0xC0C1},
		{[]byte("123456789"), 0xBB3D},
	}
	for _, tc := range cases {
		if got := crc16IBM(tc.in); got != tc.want {
			t.Errorf("crc16IBM(%q) = 0x%04X, want 0x%04X", tc.in, got, tc.want)
		}
	}
}
