package aquifer

import (
    "bytes"
    "encoding/json"
)

// UseNumber() causes the decoder to store number as json.Number,
// preserving the precisison, eg 64bit integers
func Unmarshal(data []byte, v any) error {
    dec := json.NewDecoder(bytes.NewReader(data))
    dec.UseNumber()
    return dec.Decode(v)
}
