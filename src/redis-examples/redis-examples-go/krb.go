package main

/*
// 头文件的位置，相对于源文件是当前目录，所以是 .，头文件在多个目录时写多个  #cgo CFLAGS: ...
#cgo CFLAGS: -I./include
// 从哪里加载动态库，位置与文件名，-ladd 加载 libadd.so 文件
#cgo LDFLAGS: -L./lib ./lib/libckrb5.a -lsasl2 -Wl,-rpath,lib
#include "kerberos.h"
*/
import "C"
import (
	"bufio"
	"encoding"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// BytesToString converts byte slice to string.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes converts string to byte slice.
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

func Atoi(b []byte) (int, error) {
	return strconv.Atoi(BytesToString(b))
}

func ParseInt(b []byte, base int, bitSize int) (int64, error) {
	return strconv.ParseInt(BytesToString(b), base, bitSize)
}

type writer interface {
	io.Writer
	io.ByteWriter
	// WriteString implement io.StringWriter.
	WriteString(s string) (n int, err error)
}

type Writer struct {
	writer

	lenBuf []byte
	numBuf []byte
}

func NewWriter(wr writer) *Writer {
	return &Writer{
		writer: wr,

		lenBuf: make([]byte, 64),
		numBuf: make([]byte, 64),
	}
}

func (w *Writer) WriteArgs(args []interface{}) error {
	if err := w.WriteByte(RespArray); err != nil {
		return err
	}

	if err := w.writeLen(len(args)); err != nil {
		return err
	}

	for _, arg := range args {
		if err := w.WriteArg(arg); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) writeLen(n int) error {
	w.lenBuf = strconv.AppendUint(w.lenBuf[:0], uint64(n), 10)
	w.lenBuf = append(w.lenBuf, '\r', '\n')
	_, err := w.Write(w.lenBuf)
	return err
}

func (w *Writer) WriteArg(v interface{}) error {
	switch v := v.(type) {
	case nil:
		return w.string("")
	case string:
		return w.string(v)
	case []byte:
		return w.bytes(v)
	case int:
		return w.int(int64(v))
	case int8:
		return w.int(int64(v))
	case int16:
		return w.int(int64(v))
	case int32:
		return w.int(int64(v))
	case int64:
		return w.int(v)
	case uint:
		return w.uint(uint64(v))
	case uint8:
		return w.uint(uint64(v))
	case uint16:
		return w.uint(uint64(v))
	case uint32:
		return w.uint(uint64(v))
	case uint64:
		return w.uint(v)
	case float32:
		return w.float(float64(v))
	case float64:
		return w.float(v)
	case bool:
		if v {
			return w.int(1)
		}
		return w.int(0)
	case time.Time:
		w.numBuf = v.AppendFormat(w.numBuf[:0], time.RFC3339Nano)
		return w.bytes(w.numBuf)
	case time.Duration:
		return w.int(v.Nanoseconds())
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		return w.bytes(b)
	case net.IP:
		return w.bytes(v)
	default:
		return fmt.Errorf(
			"redis: can't marshal %T (implement encoding.BinaryMarshaler)", v)
	}
}

func (w *Writer) bytes(b []byte) error {
	if err := w.WriteByte(RespString); err != nil {
		return err
	}

	if err := w.writeLen(len(b)); err != nil {
		return err
	}

	if _, err := w.Write(b); err != nil {
		return err
	}

	return w.crlf()
}

func (w *Writer) string(s string) error {
	return w.bytes(StringToBytes(s))
}

func (w *Writer) uint(n uint64) error {
	w.numBuf = strconv.AppendUint(w.numBuf[:0], n, 10)
	return w.bytes(w.numBuf)
}

func (w *Writer) int(n int64) error {
	w.numBuf = strconv.AppendInt(w.numBuf[:0], n, 10)
	return w.bytes(w.numBuf)
}

func (w *Writer) float(f float64) error {
	w.numBuf = strconv.AppendFloat(w.numBuf[:0], f, 'f', -1, 64)
	return w.bytes(w.numBuf)
}

func (w *Writer) crlf() error {
	if err := w.WriteByte('\r'); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

// redis resp protocol data type.
const (
	RespStatus    = '+' // +<string>\r\n
	RespError     = '-' // -<string>\r\n
	RespString    = '$' // $<length>\r\n<bytes>\r\n
	RespInt       = ':' // :<number>\r\n
	RespNil       = '_' // _\r\n
	RespFloat     = ',' // ,<floating-point-number>\r\n (golang float)
	RespBool      = '#' // true: #t\r\n false: #f\r\n
	RespBlobError = '!' // !<length>\r\n<bytes>\r\n
	RespVerbatim  = '=' // =<length>\r\nFORMAT:<bytes>\r\n
	RespBigInt    = '(' // (<big number>\r\n
	RespArray     = '*' // *<len>\r\n... (same as resp2)
	RespMap       = '%' // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
	RespSet       = '~' // ~<len>\r\n... (same as Array)
	RespAttr      = '|' // |<len>\r\n(key)\r\n(value)\r\n... + command reply
	RespPush      = '>' // ><len>\r\n... (same as Array)
)

// Not used temporarily.
// Redis has not used these two data types for the time being, and will implement them later.
// Streamed           = "EOF:"
// StreamedAggregated = '?'

//------------------------------------------------------------------------------

const Nil = RedisError("redis: nil") // nolint:errname

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}

func ParseErrorReply(line []byte) error {
	return RedisError(line[1:])
}

//------------------------------------------------------------------------------

type Reader struct {
	rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReader(rd),
	}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

func (r *Reader) Peek(n int) ([]byte, error) {
	return r.rd.Peek(n)
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

// PeekReplyType returns the data type of the next response without advancing the Reader,
// and discard the attribute type.
func (r *Reader) PeekReplyType() (byte, error) {
	b, err := r.rd.Peek(1)
	if err != nil {
		return 0, err
	}
	if b[0] == RespAttr {
		if err = r.DiscardNext(); err != nil {
			return 0, err
		}
		return r.PeekReplyType()
	}
	return b[0], nil
}

// ReadLine Return a valid reply, it will check the protocol or redis error,
// and discard the attribute type.
func (r *Reader) ReadLine() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case RespError:
		return nil, ParseErrorReply(line)
	case RespNil:
		return nil, Nil
	case RespBlobError:
		var blobErr string
		blobErr, err = r.readStringReply(line)
		if err == nil {
			err = RedisError(blobErr)
		}
		return nil, err
	case RespAttr:
		if err = r.Discard(line); err != nil {
			return nil, err
		}
		return r.ReadLine()
	}

	// Compatible with RESP2
	if IsNilReply(line) {
		return nil, Nil
	}

	return line, nil
}

// readLine returns an error if:
//   - there is a pending read error;
//   - or line does not end with \r\n.
func (r *Reader) readLine() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}

		full := make([]byte, len(b))
		copy(full, b)

		b, err = r.rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		full = append(full, b...) //nolint:makezero
		b = full
	}
	if len(b) <= 2 || b[len(b)-1] != '\n' || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("redis: invalid reply: %q", b)
	}
	return b[:len(b)-2], nil
}

func (r *Reader) ReadReply() (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case RespStatus:
		return string(line[1:]), nil
	case RespInt:
		return ParseInt(line[1:], 10, 64)
	case RespFloat:
		return r.readFloat(line)
	case RespBool:
		return r.readBool(line)
	case RespBigInt:
		return r.readBigInt(line)

	case RespString:
		return r.readStringReply(line)
	case RespVerbatim:
		return r.readVerb(line)

	case RespArray, RespSet, RespPush:
		return r.readSlice(line)
	case RespMap:
		return r.readMap(line)
	}
	return nil, fmt.Errorf("redis: can't parse %.100q", line)
}

func (r *Reader) readFloat(line []byte) (float64, error) {
	v := string(line[1:])
	switch string(line[1:]) {
	case "inf":
		return math.Inf(1), nil
	case "-inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(v, 64)
}

func (r *Reader) readBool(line []byte) (bool, error) {
	switch string(line[1:]) {
	case "t":
		return true, nil
	case "f":
		return false, nil
	}
	return false, fmt.Errorf("redis: can't parse bool reply: %q", line)
}

func (r *Reader) readBigInt(line []byte) (*big.Int, error) {
	i := new(big.Int)
	if i, ok := i.SetString(string(line[1:]), 10); ok {
		return i, nil
	}
	return nil, fmt.Errorf("redis: can't parse bigInt reply: %q", line)
}

func (r *Reader) readStringReply(line []byte) (string, error) {
	n, err := replyLen(line)
	if err != nil {
		return "", err
	}

	b := make([]byte, n+2)
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return "", err
	}

	return BytesToString(b[:n]), nil
}

func (r *Reader) readVerb(line []byte) (string, error) {
	s, err := r.readStringReply(line)
	if err != nil {
		return "", err
	}
	if len(s) < 4 || s[3] != ':' {
		return "", fmt.Errorf("redis: can't parse verbatim string reply: %q", line)
	}
	return s[4:], nil
}

func (r *Reader) readSlice(line []byte) ([]interface{}, error) {
	n, err := replyLen(line)
	if err != nil {
		return nil, err
	}

	val := make([]interface{}, n)
	for i := 0; i < len(val); i++ {
		v, err := r.ReadReply()
		if err != nil {
			if err == Nil {
				val[i] = nil
				continue
			}
			if err, ok := err.(RedisError); ok {
				val[i] = err
				continue
			}
			return nil, err
		}
		val[i] = v
	}
	return val, nil
}

func (r *Reader) readMap(line []byte) (map[interface{}]interface{}, error) {
	n, err := replyLen(line)
	if err != nil {
		return nil, err
	}
	m := make(map[interface{}]interface{}, n)
	for i := 0; i < n; i++ {
		k, err := r.ReadReply()
		if err != nil {
			return nil, err
		}
		v, err := r.ReadReply()
		if err != nil {
			if err == Nil {
				m[k] = nil
				continue
			}
			if err, ok := err.(RedisError); ok {
				m[k] = err
				continue
			}
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

// -------------------------------

func (r *Reader) ReadInt() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespInt, RespStatus:
		return ParseInt(line[1:], 10, 64)
	case RespString:
		s, err := r.readStringReply(line)
		if err != nil {
			return 0, err
		}
		return ParseInt([]byte(s), 10, 64)
	case RespBigInt:
		b, err := r.readBigInt(line)
		if err != nil {
			return 0, err
		}
		if !b.IsInt64() {
			return 0, fmt.Errorf("bigInt(%s) value out of range", b.String())
		}
		return b.Int64(), nil
	}
	return 0, fmt.Errorf("redis: can't parse int reply: %.100q", line)
}

func (r *Reader) ReadFloat() (float64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespFloat:
		return r.readFloat(line)
	case RespStatus:
		return strconv.ParseFloat(string(line[1:]), 64)
	case RespString:
		s, err := r.readStringReply(line)
		if err != nil {
			return 0, err
		}
		return strconv.ParseFloat(s, 64)
	}
	return 0, fmt.Errorf("redis: can't parse float reply: %.100q", line)
}

func (r *Reader) ReadString() (string, error) {
	line, err := r.ReadLine()
	if err != nil {
		return "", err
	}

	switch line[0] {
	case RespStatus, RespInt, RespFloat:
		return string(line[1:]), nil
	case RespString:
		return r.readStringReply(line)
	case RespBool:
		b, err := r.readBool(line)
		return strconv.FormatBool(b), err
	case RespVerbatim:
		return r.readVerb(line)
	case RespBigInt:
		b, err := r.readBigInt(line)
		if err != nil {
			return "", err
		}
		return b.String(), nil
	}
	return "", fmt.Errorf("redis: can't parse reply=%.100q reading string", line)
}

func (r *Reader) ReadBool() (bool, error) {
	s, err := r.ReadString()
	if err != nil {
		return false, err
	}
	return s == "OK" || s == "1" || s == "true", nil
}

func (r *Reader) ReadSlice() ([]interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	return r.readSlice(line)
}

// ReadFixedArrayLen read fixed array length.
func (r *Reader) ReadFixedArrayLen(fixedLen int) error {
	n, err := r.ReadArrayLen()
	if err != nil {
		return err
	}
	if n != fixedLen {
		return fmt.Errorf("redis: got %d elements in the array, wanted %d", n, fixedLen)
	}
	return nil
}

// ReadArrayLen Read and return the length of the array.
func (r *Reader) ReadArrayLen() (int, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespArray, RespSet, RespPush:
		return replyLen(line)
	default:
		return 0, fmt.Errorf("redis: can't parse array/set/push reply: %.100q", line)
	}
}

// ReadFixedMapLen reads fixed map length.
func (r *Reader) ReadFixedMapLen(fixedLen int) error {
	n, err := r.ReadMapLen()
	if err != nil {
		return err
	}
	if n != fixedLen {
		return fmt.Errorf("redis: got %d elements in the map, wanted %d", n, fixedLen)
	}
	return nil
}

// ReadMapLen reads the length of the map type.
// If responding to the array type (RespArray/RespSet/RespPush),
// it must be a multiple of 2 and return n/2.
// Other types will return an error.
func (r *Reader) ReadMapLen() (int, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespMap:
		return replyLen(line)
	case RespArray, RespSet, RespPush:
		// Some commands and RESP2 protocol may respond to array types.
		n, err := replyLen(line)
		if err != nil {
			return 0, err
		}
		if n%2 != 0 {
			return 0, fmt.Errorf("redis: the length of the array must be a multiple of 2, got: %d", n)
		}
		return n / 2, nil
	default:
		return 0, fmt.Errorf("redis: can't parse map reply: %.100q", line)
	}
}

// DiscardNext read and discard the data represented by the next line.
func (r *Reader) DiscardNext() error {
	line, err := r.readLine()
	if err != nil {
		return err
	}
	return r.Discard(line)
}

// Discard the data represented by line.
func (r *Reader) Discard(line []byte) (err error) {
	if len(line) == 0 {
		return errors.New("redis: invalid line")
	}
	switch line[0] {
	case RespStatus, RespError, RespInt, RespNil, RespFloat, RespBool, RespBigInt:
		return nil
	}

	n, err := replyLen(line)
	if err != nil && err != Nil {
		return err
	}

	switch line[0] {
	case RespBlobError, RespString, RespVerbatim:
		// +\r\n
		_, err = r.rd.Discard(n + 2)
		return err
	case RespArray, RespSet, RespPush:
		for i := 0; i < n; i++ {
			if err = r.DiscardNext(); err != nil {
				return err
			}
		}
		return nil
	case RespMap, RespAttr:
		// Read key & value.
		for i := 0; i < n*2; i++ {
			if err = r.DiscardNext(); err != nil {
				return err
			}
		}
		return nil
	}

	return fmt.Errorf("redis: can't parse %.100q", line)
}

func replyLen(line []byte) (n int, err error) {
	n, err = Atoi(line[1:])
	if err != nil {
		return 0, err
	}

	if n < -1 {
		return 0, fmt.Errorf("redis: invalid reply: %q", line)
	}

	switch line[0] {
	case RespString, RespVerbatim, RespBlobError,
		RespArray, RespSet, RespPush, RespMap, RespAttr:
		if n == -1 {
			return 0, Nil
		}
	}
	return n, nil
}

// IsNilReply detects redis.Nil of RESP2.
func IsNilReply(line []byte) bool {
	return len(line) == 3 &&
		(line[0] == RespString || line[0] == RespArray) &&
		line[1] == '-' && line[2] == '1'
}

func sendAuth(wr *Writer, bw *bufio.Writer, conn net.Conn, ticket *C.char, ticketLen C.int) {
	if bw.Buffered() > 0 {
		bw.Reset(conn)
	}
	goLen := int(ticketLen)
	var goStr = ""
	if goLen != 0 {
		goStr = C.GoStringN(ticket, ticketLen)
	}

	_ = wr.WriteByte(RespArray)
	_ = wr.writeLen(2)
	_ = wr.bytes([]byte("authext"))
	_ = wr.WriteByte(RespString)
	_ = wr.writeLen(goLen)
	if goLen != 0 {
		_, _ = wr.Write([]byte(goStr))
	}
	_ = wr.crlf()
	err := bw.Flush()
	if err != nil {
		fmt.Println("send error", err)
	}
}

func sendCommand(wr *Writer, bw *bufio.Writer, conn net.Conn, params []string) {
	if bw.Buffered() > 0 {
		bw.Reset(conn)
	}
	_ = wr.WriteByte(RespArray)
	_ = wr.writeLen(len(params))
	var i int
	for i = 0; i < len(params); i++ {
		_ = wr.WriteByte(RespString)
		_ = wr.writeLen(len(params[i]))
		_, _ = wr.Write([]byte(params[i]))
		_ = wr.crlf()
	}
	err := bw.Flush()
	if err != nil {
		fmt.Println("send error", err)
	}
}

// 开始认证需要调用次此函数
func authServer(serverRealm string, wr *Writer, bw *bufio.Writer, conn net.Conn, reader *Reader) (string, error) {
	fullServerRealm := C.CString(serverRealm)
	// 调用start函数，开启认证，产生客户端ticket
	result := C.start(fullServerRealm, &state)
	// 将客户端ticket发送给服务端
	sendAuth(wr, bw, conn, result.response, result.len)
	// 获取服务端ticket
	serverMsg, err := reader.ReadReply()
	if err != nil {
		fmt.Println("reply:", err)
		return "", err
	}
	serverTicket := fmt.Sprintf("%v", serverMsg)
	// 继续进行认证
	return authServerStep(serverTicket, wr, bw, conn, reader)
}

// 此函数需要递归调用，直到服务端返回authok，才算认证通过
func authServerStep(serverTicket string, wr *Writer, bw *bufio.Writer, conn net.Conn, reader *Reader) (string, error) {
	if serverTicket == "authok" {
		return "authok", nil
	}
	// 服务端ticket会默认加前缀：act:，默认要去掉
	if strings.HasPrefix(serverTicket, "act:") {
		serverTicket = serverTicket[4:]
	}
	sTicket := C.CString(serverTicket)
	sLen := len(serverTicket)
	sTLen := *(*C.ulong)(unsafe.Pointer(&sLen))
	// 调用step校验服务端ticket
	result := C.step(sTicket, sTLen, &state)
	if result.code == -1 {
		goStr := C.GoStringN(result.message, 10)
		return "", errors.New(goStr)
	}
	// 将新的客户端ticket发送给服务端
	sendAuth(wr, bw, conn, result.response, result.len)
	// 获取服务端校验结果：认证通过返回authok；需要继续认证返回服务端ticket；认证失败返回错误
	serverMsg, err := reader.ReadReply()
	if err != nil {
		// 认证失败
		fmt.Println("reply:", err)
		return "", err
	}
	newSTicket := fmt.Sprintf("%v", serverMsg)
	// 继续认证
	return authServerStep(newSTicket, wr, bw, conn, reader)
}

// 认证信息
var goState *C.struct_client_state = &C.struct_client_state{
	auth_started:  0,
	auth_complete: 0,
}

// 转c指针
var state = *(*C.struct_client_state)(unsafe.Pointer(goState))

func main() {
	// 启动时，初始化认证环境
	C.initializeClient()
	// 格式： hadoop.域名
	var fullServerRealm = "hadoop.HADOOP.COM"
	// Redis ip端口信息
	conn, err := net.Dial("tcp", "192.168.67.169:22406")
	if err != nil {
		fmt.Println("err:", err)
		return
	}
	bw := bufio.NewWriter(conn)
	wr := NewWriter(bw)
	reader := NewReader(bufio.NewReader(conn))
	// 开始认证
	authRes, err := authServer(fullServerRealm, wr, bw, conn, reader)
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println("authRes=", authRes)
	// 认证结束，开始发送命令
	var infoCommand = []string{"info", "Server"}
	sendCommand(wr, bw, conn, infoCommand)
	serverMsg, err := reader.ReadReply()
	if err != nil {
		fmt.Println("reply:", err)
		return
	}
	info := fmt.Sprintf("%v", serverMsg)
	fmt.Println("info=", info)
}
