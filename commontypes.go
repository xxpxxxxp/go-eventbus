package eventbus

import "reflect"

var (
	BoolType    = reflect.TypeOf(false)
	IntType     = reflect.TypeOf(int(0))
	Int8Type    = reflect.TypeOf(int8(0))
	Int16Type   = reflect.TypeOf(int16(0))
	Int32Type   = reflect.TypeOf(int32(0))
	Int64Type   = reflect.TypeOf(int64(0))
	UIntType    = reflect.TypeOf(uint(0))
	UInt8Type   = reflect.TypeOf(uint8(0))
	UInt16Type  = reflect.TypeOf(uint16(0))
	UInt32Type  = reflect.TypeOf(uint32(0))
	UInt64Type  = reflect.TypeOf(uint64(0))
	ByteType    = reflect.TypeOf(byte(0))
	Float32Type = reflect.TypeOf(float32(0.0))
	Float64Type = reflect.TypeOf(float64(0.0))
	StringType  = reflect.TypeOf("")
)
