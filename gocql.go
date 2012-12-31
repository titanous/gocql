// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The gocql package provides a database/sql driver for CQL, the Cassandra
// query language.
//
// This package requires a recent version of Cassandra (≥ 1.2) that supports
// CQL 3.0 and the new native protocol. The native protocol is still considered
// beta and must be enabled manually in Cassandra 1.2 by setting
// "start_native_transport" to true in conf/cassandra.yaml.
//
// Example Usage:
//
//     db, err := sql.Open("gocql", "localhost:9042 keyspace=system")
//     // ...
//     rows, err := db.Query("SELECT keyspace_name FROM schema_keyspaces")
//     // ...
//     for rows.Next() {
//          var keyspace string
//          err = rows.Scan(&keyspace)
//          // ...
//          fmt.Println(keyspace)
//     }
//     if err := rows.Err(); err != nil {
//         // ...
//     }
//
package gocql

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"encoding/binary"
	"fmt"
	"github.com/titanous/go-backports/database/sql"
	"github.com/titanous/go-backports/database/sql/driver"
	"io"
	"math/rand"
	"net"
	"strings"
)

const (
	protoRequest  byte = 0x01
	protoResponse byte = 0x81

	opError        byte = 0x00
	opStartup      byte = 0x01
	opReady        byte = 0x02
	opAuthenticate byte = 0x03
	opCredentials  byte = 0x04
	opOptions      byte = 0x05
	opSupported    byte = 0x06
	opQuery        byte = 0x07
	opResult       byte = 0x08
	opPrepare      byte = 0x09
	opExecute      byte = 0x0A
       opLAST         byte = 0x0A // not a real opcode -- used to check for valid opcodes

	flagCompressed byte = 0x01

	keyVersion     string = "CQL_VERSION"
	keyCompression string = "COMPRESSION"
)

var consistencyLevels = map[string]byte{"any": 0x00, "one": 0x01, "two": 0x02,
	"three": 0x03, "quorum": 0x04, "all": 0x05, "local_quorum": 0x06, "each_quorum": 0x07}

var rnd = rand.New(rand.NewSource(0))

type drv struct{}

func (d drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

type connection struct {
	c           net.Conn
	compression string
	consistency byte
}

func Open(name string) (*connection, error) {
	parts := strings.Split(name, " ")
	address := ""
	if len(parts) >= 1 {
		addresses := strings.Split(parts[0], ",")
		if len(addresses) > 0 {
			address = addresses[rnd.Intn(len(addresses))]
		}
	}
	c, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	version := "3.0.0"
	var (
		keyspace    string
		compression string
		consistency byte = 0x01
		ok          bool
	)
	for i := 1; i < len(parts); i++ {
		switch {
		case parts[i] == "":
			continue
		case strings.HasPrefix(parts[i], "keyspace="):
			keyspace = strings.TrimSpace(parts[i][9:])
		case strings.HasPrefix(parts[i], "compression="):
			compression = strings.TrimSpace(parts[i][12:])
			if compression != "snappy" {
				return nil, fmt.Errorf("unknown compression algorithm %q",
					compression)
			}
		case strings.HasPrefix(parts[i], "version="):
			version = strings.TrimSpace(parts[i][8:])
		case strings.HasPrefix(parts[i], "consistency="):
			cs := strings.TrimSpace(parts[i][12:])
			if consistency, ok = consistencyLevels[cs]; !ok {
				return nil, fmt.Errorf("unknown consistency level %q", cs)
			}
		default:
			return nil, fmt.Errorf("unsupported option %q", parts[i])
		}
	}

       cn := &connection{c: c, compression: compression, consistency: consistency}

	b := &bytes.Buffer{}

	if compression != "" {
		binary.Write(b, binary.BigEndian, uint16(2))
	} else {
		binary.Write(b, binary.BigEndian, uint16(1))
	}

	binary.Write(b, binary.BigEndian, uint16(len(keyVersion)))
	b.WriteString(keyVersion)
	binary.Write(b, binary.BigEndian, uint16(len(version)))
	b.WriteString(version)

	if compression != "" {
		binary.Write(b, binary.BigEndian, uint16(len(keyCompression)))
		b.WriteString(keyCompression)
		binary.Write(b, binary.BigEndian, uint16(len(compression)))
		b.WriteString(compression)
	}

	if err := cn.send(opStartup, b.Bytes()); err != nil {
		return nil, err
	}

	opcode, _, err := cn.recv()
	if err != nil {
		return nil, err
	}
	if opcode != opReady {
		return nil, fmt.Errorf("connection not ready")
	}

	if keyspace != "" {
		st, err := cn.Prepare(fmt.Sprintf("USE %s", keyspace))
		if err != nil {
			return nil, err
		}
		if _, err = st.Exec([]driver.Value{}); err != nil {
			return nil, err
		}
	}

	return cn, nil
}

func (cn *connection) send(opcode byte, body []byte) error {
        if cn.c == nil {
                return driver.ErrBadConn
        }
	frame := make([]byte, len(body)+8)
	frame[0] = protoRequest
	frame[1] = 0
	frame[2] = 0
	frame[3] = opcode
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(body)))
	copy(frame[8:], body)
        log(cn.c, fmt.Sprintf("write op=%x sz=%d", opcode, len(frame)))
	if _, err := cn.c.Write(frame); err != nil {
		return err
	}
	return nil
}

// ReadFully reads a full byte array from a connection, issuing multiple net.conn.Read()
// calls until all the bytes have come in
func (cn *connection) readFully(b []byte) (err error) {
	for got:=0; got<int(len(b)); {
		new, err := cn.c.Read(b[got:])
		if err != nil { return err }
		got += new
	}
	return nil
}

func (cn *connection) recv() (byte, []byte, error) {
        defer func(){
                if r := recover(); r != nil { LogPanic(r) }
        }()
        if cn.c == nil {
                return 0, nil, driver.ErrBadConn
        }
	header := make([]byte, 8)
        log(cn.c, fmt.Sprintf("read_"))
	if err := cn.readFully(header); err != nil {
                log(cn.c, fmt.Sprintf("readH err=%s", err.Error()))
		return 0, nil, err
	}
        log(cn.c, fmt.Sprintf("read 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x 0x%x",
                header[0], header[1], header[2], header[3], header[4],
                header[5], header[6], header[7]))
        if header[0] != protoResponse {
                // Seeing errors using Ubuntu 10.04 such as:
                // Error reading prepare result for query <<select data from agg where
                // slt = ?>> -- (unsupported frame version or not a response: 0xfe
                // (header=[254 248 127 1 254 248 127 0]))
                // Error reading prepare result for query <<select data from agg where
                // slt = ?>> -- (unsupported frame version or not a response: 0x0
                // (header=[0 0 0 0 0 0 0 0]))
                log(cn.c, fmt.Sprintf("close"))
                cn.c.Close()
                cn.c = nil // ensure we generate ErrBadConn
                return 0, nil, fmt.Errorf("unsupported frame version or not a response: 0x%x (header=%v)", header[0], header)
        }
        if header[1] > 1 {
               // this is overly conservative, but really helps catch correputed framing
                log(cn.c, fmt.Sprintf("close"))
                cn.c.Close()
                cn.c = nil // ensure we generate ErrBadConn
                return 0, nil, fmt.Errorf("unsupported frame flags: 0x%x (header=%v)", header[1], header)
        }
	opcode := header[3]
        if opcode > opLAST {
                log(cn.c, fmt.Sprintf("close"))
                cn.c.Close()
                cn.c = nil // ensure we generate ErrBadConn
                return 0, nil, fmt.Errorf("unknown opcode: 0x%x (header=%v)", opcode, header)
        }
	length := binary.BigEndian.Uint32(header[4:8])
	var body []byte
	if length > 0 {
                if length > 256*1024*1024 { // spec says 256MB is max
                        cn.c.Close()
                        cn.c = nil // ensure we generate ErrBadConn
                        return 0, nil, fmt.Errorf("frame too large: %d (header=%v)", length, header)
                }
		body = make([]byte, length)
                log(cn.c, fmt.Sprintf("read body %dbytes", length))
		if err := cn.readFully(body); err != nil {
			return 0, nil, err
		}
		nn := 32; if len(body) < nn { nn = len(body) }
                //log(cn.c, fmt.Sprintf("body=%v", got, length, body[:nn]))
		//if got != int(length) { LogPanic(fmt.Sprintf("got %d of %d body=%v", got, length, body[:nn])) }
	}
	if header[1]&flagCompressed != 0 && cn.compression == "snappy" {
		var err error
		body, err = snappy.Decode(nil, body)
		if err != nil {
			log(cn.c, fmt.Sprintf("snappy error on %v", cn.c))
			LogPanic(fmt.Sprintf("snappy error on %v", cn.c))
                        cn.c.Close()
                        cn.c = nil // ensure we generate ErrBadConn
			return 0, nil, err
		}
	}
	if opcode == opError {
		code := binary.BigEndian.Uint32(body[0:4])
		msglen := binary.BigEndian.Uint16(body[4:6])
		msg := string(body[6 : 6+msglen])
		return opcode, body, Error{Code: int(code), Msg: msg}
	}
	return opcode, body, nil
}

func (cn *connection) Begin() (driver.Tx, error) {
        if cn.c == nil { return nil, driver.ErrBadConn }
	return cn, nil
}

func (cn *connection) Commit() error {
        if cn.c == nil { return driver.ErrBadConn }
	return nil
}

func (cn *connection) Close() error {
        if cn.c == nil { return driver.ErrBadConn }
	return cn.c.Close()
}

func (cn *connection) Rollback() error {
        if cn.c == nil { return driver.ErrBadConn }
	return nil
}

func (cn *connection) Prepare(query string) (driver.Stmt, error) {
	body := make([]byte, len(query)+4)
	binary.BigEndian.PutUint32(body[0:4], uint32(len(query)))
	copy(body[4:], []byte(query))
	if err := cn.send(opPrepare, body); err != nil {
		return nil, err
	}
	opcode, body, err := cn.recv()
	if err != nil {
                err = fmt.Errorf("Error reading prepare result for query <<%s>> -- (%s)", query, err.Error())
		return nil, err
	}
	if opcode != opResult || binary.BigEndian.Uint32(body) != 4 {
		return nil, fmt.Errorf("expected prepared result")
	}
	n := int(binary.BigEndian.Uint16(body[4:]))
	prepared := body[6 : 6+n]
	columns, meta, _ := parseMeta(body[6+n:])
	return &statement{cn: cn, query: query,
		prepared: prepared, columns: columns, meta: meta}, nil
}

type statement struct {
	cn       *connection
	query    string
	prepared []byte
	columns  []string
	meta     []uint16
}

func (s *statement) Close() error {
	return nil
}

func (st *statement) ColumnConverter(idx int) driver.ValueConverter {
	return (&columnEncoder{st.meta}).ColumnConverter(idx)
}

func (st *statement) NumInput() int {
	return len(st.columns)
}

func parseMeta(body []byte) ([]string, []uint16, int) {
	flags := binary.BigEndian.Uint32(body)
	globalTableSpec := flags&1 == 1
	columnCount := int(binary.BigEndian.Uint32(body[4:]))
	i := 8
	if globalTableSpec {
		l := int(binary.BigEndian.Uint16(body[i:]))
		keyspace := string(body[i+2 : i+2+l])
		i += 2 + l
		l = int(binary.BigEndian.Uint16(body[i:]))
		tablename := string(body[i+2 : i+2+l])
		i += 2 + l
		_, _ = keyspace, tablename
	}
	columns := make([]string, columnCount)
	meta := make([]uint16, columnCount)
	for c := 0; c < columnCount; c++ {
		l := int(binary.BigEndian.Uint16(body[i:]))
		columns[c] = string(body[i+2 : i+2+l])
		i += 2 + l
		meta[c] = binary.BigEndian.Uint16(body[i:])
		i += 2
	}
	return columns, meta, i
}

func (st *statement) exec(v []driver.Value) error {
	sz := 6 + len(st.prepared)
	for i := range v {
		if b, ok := v[i].([]byte); ok {
			sz += len(b) + 4
		}
	}
	body, p := make([]byte, sz), 4+len(st.prepared)
	binary.BigEndian.PutUint16(body, uint16(len(st.prepared)))
	copy(body[2:], st.prepared)
	binary.BigEndian.PutUint16(body[p-2:], uint16(len(v)))
	for i := range v {
		b, ok := v[i].([]byte)
		if !ok {
			return fmt.Errorf("unsupported type %T at column %d", v[i], i)
		}
		binary.BigEndian.PutUint32(body[p:], uint32(len(b)))
		copy(body[p+4:], b)
		p += 4 + len(b)
	}
	binary.BigEndian.PutUint16(body[p:], uint16(st.cn.consistency))
	if err := st.cn.send(opExecute, body); err != nil {
		return err
	}
	return nil
}

func (st *statement) Exec(v []driver.Value) (driver.Result, error) {
	if err := st.exec(v); err != nil {
		return nil, err
	}
	opcode, body, err := st.cn.recv()
	if err != nil {
		return nil, err
	}
	_, _ = opcode, body
	return nil, nil
}

func (st *statement) Query(v []driver.Value) (driver.Rows, error) {
	if err := st.exec(v); err != nil {
		return nil, err
	}
	opcode, body, err := st.cn.recv()
	if err != nil {
		return nil, err
	}
	kind := binary.BigEndian.Uint32(body[0:4])
	if opcode != opResult || kind != 2 {
		return nil, fmt.Errorf("expected rows as result")
	}
	columns, meta, n := parseMeta(body[4:])
	i := n + 4
	rows := &rows{
		columns: columns,
		meta:    meta,
		numRows: int(binary.BigEndian.Uint32(body[i:])),
		conn:    st.cn.c,
		orig:    body,
	}
	i += 4
	rows.body = body[i:]
	log(st.cn.c, fmt.Sprintf("rows %v", rows))
	return rows, nil
}

type rows struct {
	columns []string
	meta    []uint16
	body    []byte
	row     int
	numRows int
	conn	net.Conn
	orig	[]byte
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Next(values []driver.Value) error {
	var info string
        defer func(){
                if p := recover(); p != nil {
			info += fmt.Sprintf("row: %v %v %d %d\n",r.columns,r.meta,r.row,r.numRows)
			for i:=0; i<len(r.orig); i+=16 {
				info += fmt.Sprintf("\n0x%04x: ", i)
				for j:=0; j<16; j+=2 {
					if i+j < len(r.orig) {
						info += fmt.Sprintf(" %02x", r.orig[i+j])
					}
					if i+j+1 < len(r.orig) {
						info += fmt.Sprintf("%02x", r.orig[i+j+1])
					}
				}
			}
			info += "\n"
			
			fmt.Println(info)
			LogPanic(p)
		}
        }()
	if r.row >= r.numRows {
		return io.EOF
	}
        b0 := r.body
	for column := 0; column < len(r.columns); column++ {
		info = fmt.Sprintf("%s c=%d/%d b=%d/%d orig=%d",
			r.conn.LocalAddr().String(), column, len(r.columns),
			len(b0)-len(r.body), len(b0), len(r.orig))
		n := int(binary.BigEndian.Uint32(r.body))
		r.body = r.body[4:]
		info += fmt.Sprintf(" t=0x%x n=%d bytes=%v addr=%s\n",
			r.meta[column], n, r.body[:n], r.conn.LocalAddr().String())
		if n >= 0 {
			values[column] = decode(r.body[:n], r.meta[column])
			r.body = r.body[n:]
		} else {
			values[column] = nil
		}
	}
	r.row++
	return nil
}

type Error struct {
	Code int
	Msg  string
}

func (e Error) Error() string {
	return e.Msg
}

func init() {
        go logBunny()
	sql.Register("gocql", &drv{})
}

//=== troubleshooting

type logMsg struct {
	p string
        c net.Conn
        msg string
}
var logChan = make(chan logMsg, 100)
var logList = make([]*logMsg, 0)

func log(c net.Conn, msg string) {
        logChan <- logMsg{c.LocalAddr().String(), c, msg}
}

func logBunny() {
        for {
                m := <-logChan
                logList = append(logList, &m)
                if len(logList) > 100 {
                        logList = logList[1:]
                }
        }
}

func LogPanic(what interface{}) {
	L: for {
		select {
		case m := <-logChan:
			fmt.Printf("EXTRA: %s: %s\n", m.p, m.msg)
			logList = append(logList, &m)
		default:
			break L
		}
	}
        for i,l := range logList {
                fmt.Printf("LOG%02d %s: %s\n", i, l.p, l.msg)
        }
	logList = make([]*logMsg, 0)
        panic(what)
}
