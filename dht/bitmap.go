package dht

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"strconv"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lyoshenka/bencode"
)

// TODO: http://roaringbitmap.org/

// Bitmap is a generalized representation of an identifier or data that can be sorted, compared fast. Used by the DHT
// package as a way to handle the unique identifiers of a DHT node.
type Bitmap [nodeIDLength]byte

func (b Bitmap) rawString() string {
	return string(b[:])
}

// BString returns the bitmap as a string of 0s and 1s
func (b Bitmap) BString() string {
	var buf bytes.Buffer
	for i := 0; i < nodeIDBits; i++ {
		if b.Get(i) {
			buf.WriteString("1")
		} else {
			buf.WriteString("0")
		}
	}
	return buf.String()
}

// Hex returns a hexadecimal representation of the bitmap.
func (b Bitmap) Hex() string {
	return hex.EncodeToString(b[:])
}

// HexShort returns a hexadecimal representation of the first 4 bytes.
func (b Bitmap) HexShort() string {
	return hex.EncodeToString(b[:4])
}

// HexSimplified returns the hexadecimal representation with all leading 0's removed
func (b Bitmap) HexSimplified() string {
	simple := strings.TrimLeft(b.Hex(), "0")
	if simple == "" {
		simple = "0"
	}
	return simple
}

// Equals returns T/F if every byte in bitmap are equal.
func (b Bitmap) Equals(other Bitmap) bool {
	for k := range b {
		if b[k] != other[k] {
			return false
		}
	}
	return true
}

// Less returns T/F if there exists a byte pair that is not equal AND this bitmap is less than the other.
func (b Bitmap) Less(other interface{}) bool {
	for k := range b {
		if b[k] != other.(Bitmap)[k] {
			return b[k] < other.(Bitmap)[k]
		}
	}
	return false
}

// LessOrEqual returns true if the bitmaps are equal, otherwise it checks if this bitmap is less than the other.
func (b Bitmap) LessOrEqual(other interface{}) bool {
	if bm, ok := other.(Bitmap); ok && b.Equals(bm) {
		return true
	}
	return b.Less(other)
}

// Greater returns T/F if there exists a byte pair that is not equal AND this bitmap byte is greater than the other.
func (b Bitmap) Greater(other interface{}) bool {
	for k := range b {
		if b[k] != other.(Bitmap)[k] {
			return b[k] > other.(Bitmap)[k]
		}
	}
	return false
}

// GreaterOrEqual returns true if the bitmaps are equal, otherwise it checks if this bitmap is greater than the other.
func (b Bitmap) GreaterOrEqual(other interface{}) bool {
	if bm, ok := other.(Bitmap); ok && b.Equals(bm) {
		return true
	}
	return b.Greater(other)
}

// Copy returns a duplicate value for the bitmap.
func (b Bitmap) Copy() Bitmap {
	var ret Bitmap
	copy(ret[:], b[:])
	return ret
}

// Xor returns a diff bitmap. If they are equal, the returned bitmap will be all 0's. If 100% unique the returned
// bitmap will be all 1's.
func (b Bitmap) Xor(other Bitmap) Bitmap {
	var ret Bitmap
	for k := range b {
		ret[k] = b[k] ^ other[k]
	}
	return ret
}

// And returns a comparison bitmap, that for each byte returns the AND true table result
func (b Bitmap) And(other Bitmap) Bitmap {
	var ret Bitmap
	for k := range b {
		ret[k] = b[k] & other[k]
	}
	return ret
}

// Or returns a comparison bitmap, that for each byte returns the OR true table result
func (b Bitmap) Or(other Bitmap) Bitmap {
	var ret Bitmap
	for k := range b {
		ret[k] = b[k] | other[k]
	}
	return ret
}

// Not returns a complimentary bitmap that is an inverse. So b.NOT.NOT = b
func (b Bitmap) Not() Bitmap {
	var ret Bitmap
	for k := range b {
		ret[k] = ^b[k]
	}
	return ret
}

func (b Bitmap) add(other Bitmap) (Bitmap, bool) {
	var ret Bitmap
	carry := false
	for i := nodeIDBits - 1; i >= 0; i-- {
		bBit := getBit(b[:], i)
		oBit := getBit(other[:], i)
		setBit(ret[:], i, bBit != oBit != carry)
		carry = (bBit && oBit) || (bBit && carry) || (oBit && carry)
	}
	return ret, carry
}

// Add returns a bitmap that treats both bitmaps as numbers and adding them together. Since the size of a bitmap is
// limited, an overflow is possible when adding bitmaps.
func (b Bitmap) Add(other Bitmap) Bitmap {
	ret, carry := b.add(other)
	if carry {
		panic("overflow in bitmap addition. limited to " + strconv.Itoa(nodeIDBits) + " bits.")
	}
	return ret
}

// Sub returns a bitmap that treats both bitmaps as numbers and subtracts then via the inverse of the other and adding
// then together a + (-b). Negative bitmaps are not supported so other must be greater than this.
func (b Bitmap) Sub(other Bitmap) Bitmap {
	if b.Less(other) {
		// ToDo: Why is this not supported? Should it say not implemented? BitMap might have a generic use case outside of dht.
		panic("negative bitmaps not supported")
	}
	complement, _ := other.Not().add(BitmapFromShortHexP("1"))
	ret, _ := b.add(complement)
	return ret
}

// Get returns the binary bit at the position passed.
func (b Bitmap) Get(n int) bool {
	return getBit(b[:], n)
}

// Set sets the binary bit at the position passed.
func (b Bitmap) Set(n int, one bool) Bitmap {
	ret := b.Copy()
	setBit(ret[:], n, one)
	return ret
}

// PrefixLen returns the number of leading 0 bits
func (b Bitmap) PrefixLen() int {
	for i := range b {
		for j := 0; j < 8; j++ {
			if (b[i]>>uint8(7-j))&0x1 != 0 {
				return i*8 + j
			}
		}
	}
	return nodeIDBits
}

// Prefix returns a copy of b with the first n bits set to 1 (if `one` is true) or 0 (if `one` is false)
// https://stackoverflow.com/a/23192263/182709
func (b Bitmap) Prefix(n int, one bool) Bitmap {
	ret := b.Copy()

Outer:
	for i := range ret {
		for j := 0; j < 8; j++ {
			if i*8+j < n {
				if one {
					ret[i] |= 1 << uint(7-j)
				} else {
					ret[i] &= ^(1 << uint(7-j))
				}
			} else {
				break Outer
			}
		}
	}

	return ret
}

// Suffix returns a copy of b with the last n bits set to 1 (if `one` is true) or 0 (if `one` is false)
// https://stackoverflow.com/a/23192263/182709
func (b Bitmap) Suffix(n int, one bool) Bitmap {
	ret := b.Copy()

Outer:
	for i := len(ret) - 1; i >= 0; i-- {
		for j := 7; j >= 0; j-- {
			if i*8+j >= nodeIDBits-n {
				if one {
					ret[i] |= 1 << uint(7-j)
				} else {
					ret[i] &= ^(1 << uint(7-j))
				}
			} else {
				break Outer
			}
		}
	}

	return ret
}

// MarshalBencode implements the Marshaller(bencode)/Message interface.
func (b Bitmap) MarshalBencode() ([]byte, error) {
	str := string(b[:])
	return bencode.EncodeBytes(str)
}

// UnmarshalBencode implements the Marshaller(bencode)/Message interface.
func (b *Bitmap) UnmarshalBencode(encoded []byte) error {
	var str string
	err := bencode.DecodeBytes(encoded, &str)
	if err != nil {
		return err
	}
	if len(str) != nodeIDLength {
		return errors.Err("invalid bitmap length")
	}
	copy(b[:], str)
	return nil
}

// BitmapFromBytes returns a bitmap as long as the byte array is of a specific length specified in the parameters.
func BitmapFromBytes(data []byte) (Bitmap, error) {
	var bmp Bitmap

	if len(data) != len(bmp) {
		return bmp, errors.Err("invalid bitmap of length %d", len(data))
	}

	copy(bmp[:], data)
	return bmp, nil
}

// BitmapFromBytesP returns a bitmap as long as the byte array is of a specific length specified in the parameters
// otherwise it wil panic.
func BitmapFromBytesP(data []byte) Bitmap {
	bmp, err := BitmapFromBytes(data)
	if err != nil {
		panic(err)
	}
	return bmp
}

//BitmapFromString returns a bitmap by converting the string to bytes and creating from bytes as long as the byte array
// is of a specific length specified in the parameters
func BitmapFromString(data string) (Bitmap, error) {
	return BitmapFromBytes([]byte(data))
}

//BitmapFromStringP returns a bitmap by converting the string to bytes and creating from bytes as long as the byte array
// is of a specific length specified in the parameters otherwise it wil panic.
func BitmapFromStringP(data string) Bitmap {
	bmp, err := BitmapFromString(data)
	if err != nil {
		panic(err)
	}
	return bmp
}

//BitmapFromHex returns a bitmap by converting the hex string to bytes and creating from bytes as long as the byte array
// is of a specific length specified in the parameters
func BitmapFromHex(hexStr string) (Bitmap, error) {
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return Bitmap{}, errors.Err(err)
	}
	return BitmapFromBytes(decoded)
}

//BitmapFromHexP returns a bitmap by converting the hex string to bytes and creating from bytes as long as the byte array
// is of a specific length specified in the parameters otherwise it wil panic.
func BitmapFromHexP(hexStr string) Bitmap {
	bmp, err := BitmapFromHex(hexStr)
	if err != nil {
		panic(err)
	}
	return bmp
}

//BitmapFromShortHex returns a bitmap by converting the hex string to bytes, adding the leading zeros prefix to the
// hex string and creating from bytes as long as the byte array is of a specific length specified in the parameters
func BitmapFromShortHex(hexStr string) (Bitmap, error) {
	return BitmapFromHex(strings.Repeat("0", nodeIDLength*2-len(hexStr)) + hexStr)
}

//BitmapFromShortHexP returns a bitmap by converting the hex string to bytes, adding the leading zeros prefix to the
// hex string and creating from bytes as long as the byte array is of a specific length specified in the parameters
// otherwise it wil panic.
func BitmapFromShortHexP(hexStr string) Bitmap {
	bmp, err := BitmapFromShortHex(hexStr)
	if err != nil {
		panic(err)
	}
	return bmp
}

// RandomBitmapP generates a cryptographically random bitmap with the confines of the parameters specified.
func RandomBitmapP() Bitmap {
	var id Bitmap
	_, err := rand.Read(id[:])
	if err != nil {
		panic(err)
	}
	return id
}

// RandomBitmapInRangeP generates a cryptographically random bitmap and while it is greater than the high threshold
// bitmap will subtract the diff between high and low until it is no longer greater that the high.
func RandomBitmapInRangeP(low, high Bitmap) Bitmap {
	diff := high.Sub(low)
	r := RandomBitmapP()
	for r.Greater(diff) {
		r = r.Sub(diff)
	}
	//ToDo - Adding the low at this point doesn't gurantee it will be within the range. Consider bitmaps as numbers and
	// I have a range of 50-100. If get to say 60, and add 50, I would be at 110. Should protect against this?
	return r.Add(low)
}

func getBit(b []byte, n int) bool {
	i := n / 8
	j := n % 8
	return b[i]&(1<<uint(7-j)) > 0
}

func setBit(b []byte, n int, one bool) {
	i := n / 8
	j := n % 8
	if one {
		b[i] |= 1 << uint(7-j)
	} else {
		b[i] &= ^(1 << uint(7-j))
	}
}
