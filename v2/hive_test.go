//go:build all || integration
// +build all integration

package gohive

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letters = []rune("abcdefghijklmnopqrstuvwxyz")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

var tableId = 0
var randName = randSeq(10)

func TestConnectDefault(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	auth := os.Getenv("AUTH")
	ssl := os.Getenv("SSL")
	if auth != "KERBEROS" || transport != "binary" || ssl == "1" {
		t.Skip("not testing this combination.")
	}

	configuration := newConnectConfiguration()
	configuration.Service = "hive"
	connection, err := connect(context.Background(), "hs2.example.com", 10000, getAuth(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	connection.close()
}

func TestDomainDoesntExist(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	auth := os.Getenv("AUTH")
	ssl := os.Getenv("SSL")
	if auth != "KERBEROS" || transport != "binary" || ssl == "1" {
		return
	}

	configuration := newConnectConfiguration()
	configuration.Service = "hive"
	_, err := connect(context.Background(), "nonexistentdomain", 10000, getAuth(), configuration)
	if err == nil {
		t.Fatal("Expected error because domain doesn't exist")
	}
}

func TestConnectDigestMd5(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	auth := os.Getenv("AUTH")
	ssl := os.Getenv("SSL")
	if auth != "NONE" || transport != "binary" || ssl == "1" {
		return
	}

	configuration := newConnectConfiguration()
	configuration.Service = "null"
	configuration.Password = "pass"
	configuration.Username = "hive"
	_, err := connect(context.Background(), "hs2.example.com", 10000, "DIGEST-MD5", configuration)
	if err == nil {
		t.Fatal("Error was expected because the server won't accept this mechanism")
	}
}

func TestResuseConnection(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	cursor.exec(context.Background(), "SHOW DATABASES")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	cursor.close(context.Background())

	newCursor := connection.cursor()
	cursor.exec(context.Background(), "SHOW DATABASES")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	closeAll(t, connection, newCursor)
}

func TestConnectHttp(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	ssl := os.Getenv("SSL")
	if transport != "http" {
		return
	}
	configuration := newConnectConfiguration()
	configuration.TransportMode = transport
	configuration.Service = "hive"
	if ssl == "1" {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}
	connection, err := connect(context.Background(), "hs2.example.com", 10000, getAuth(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	connection.close()
}

func TestConnectSasl(t *testing.T) {
	transport := os.Getenv("TRANSPORT")
	ssl := os.Getenv("SSL")
	if transport != "binary" {
		return
	}
	configuration := newConnectConfiguration()
	configuration.TransportMode = "binary"
	configuration.Service = "hive"
	if ssl == "1" {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}
	connection, err := connect(context.Background(), "hs2.example.com", 10000, getAuth(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	connection.close()
}

func TestClosedPort(t *testing.T) {
	configuration := newConnectConfiguration()
	configuration.Service = "hive"
	_, err := connect(context.Background(), "hs2.example.com", 12345, getAuth(), configuration)
	if err == nil {
		t.Fatal(err)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("Wrong error: %s, it should contain connection refused", err.Error())
	}
}

func TestCreateTable(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	cursor.exec(context.Background(), "DROP TABLE IF EXISTS pokes6")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.exec(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	// Now it should fail because the table already exists
	cursor.exec(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)")
	if cursor.error() == nil {
		t.Fatal("Error should have happened")
	}
	closeAll(t, connection, cursor)
}

func TestManyFailures(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	cursor.exec(context.Background(), "DROP TABLE IF EXISTS pokes6")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.exec(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	for i := 0; i < 20; i++ {
		// Now it should fail because the table already exists
		cursor.exec(context.Background(), "CREATE TABLE pokes6 (foo INT, bar INT)")
		if cursor.error() == nil {
			t.Fatal("Error should have happened")
		}
	}

	closeAll(t, connection, cursor)
}

func TestDescription(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 2, 1000)

	// We come from an insert
	d := cursor.description(context.Background())
	expected := [][]string{[]string{"col1", "INT_TYPE"}, []string{"col2", "STRING_TYPE"}}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	d = cursor.description(context.Background())
	expected = [][]string{[]string{fmt.Sprintf("%s.a", tableName), "INT_TYPE"}, []string{fmt.Sprintf("%s.b", tableName), "STRING_TYPE"}}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	var i int32
	var s string
	cursor.fetchOne(context.Background(), &i, &s)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	d = cursor.description(context.Background())
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	expected = [][]string{[]string{fmt.Sprintf("%s.a", tableName), "INT_TYPE"}, []string{fmt.Sprintf("%s.b", tableName), "STRING_TYPE"}}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}

	// Call again it will follow a different path
	d = cursor.description(context.Background())
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	closeAll(t, connection, cursor)
}

func TestSelect(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 6000, 1000)

	var i int32
	var s string
	var j int32
	var z int

	for z, j = 0, 0; z < 10; z, j, i, s = z+1, 0, 0, "-1" {
		cursor.exec(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s", tableName))
		cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
		for cursor.hasMore(context.Background()) {
			if cursor.error() != nil {
				t.Fatal(cursor.error())
			}
			cursor.fetchOne(context.Background(), &i, &s)
			if cursor.error() != nil {
				t.Fatal(cursor.error())
			}
			j++
		}
		if i != 6000 || s != "6000" {
			log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
		}
		if cursor.hasMore(context.Background()) {
			log.Fatal("Shouldn't have any more values")
		}
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
		if j != 6000 {
			t.Fatalf("6000 rows expected here, found %d", j)
		}
	}
	closeAll(t, connection, cursor)
}

func TestSetDatabaseConfig(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	cursor.exec(context.Background(), "DROP TABLE IF EXISTS datbas.dpokes")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	cursor.exec(context.Background(), "DROP TABLE IF EXISTS dpokes")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.exec(context.Background(), "CREATE DATABASE IF NOT EXISTS datbas")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	configuration := newConnectConfiguration()
	configuration.Database = "datbas"
	configuration.Service = "hive"
	configuration.FetchSize = 1000
	configuration.TransportMode = getTransport()
	configuration.HiveConfiguration = nil

	connection, cursor = makeConnectionWithConnectConfiguration(t, configuration)

	cursor.exec(context.Background(), "CREATE TABLE datbas.dpokes (foo INT, bar INT)")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.exec(context.Background(), "INSERT INTO dpokes VALUES(1, 1111)")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.exec(context.Background(), "SELECT * FROM dpokes")
	m := cursor.rowMap(context.Background())
	expected := map[string]interface{}{"dpokes.foo": int32(1), "dpokes.bar": int32(1111)}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	cursor.exec(context.Background(), "SELECT * FROM datbas.dpokes")
	m = cursor.rowMap(context.Background())
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	cursor.exec(context.Background(), "DROP TABLE IF EXISTS datbas.dpokes")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.exec(context.Background(), "DROP DATABASE IF EXISTS datbas")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	closeAll(t, connection, cursor)
}

func TestSelectNull(t *testing.T) {
	connection, cursor, tableName := prepareTableSingleValue(t, 6000, 1000)
	cursor.exec(context.Background(), fmt.Sprintf("INSERT into %s(a) values(1);", tableName))
	closeAll(t, connection, cursor)

	connection, cursor = makeConnection(t, 199)

	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	j := 0
	for cursor.hasMore(context.Background()) {
		var i *int32 = new(int32)
		var s *string = new(string)
		*i = 1
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
		cursor.fetchOne(context.Background(), &i, &s)
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
		if i != nil {
			log.Fatalf("Unexpected value for i: %d", *i)
		}
		if j == 6000 {
			if *i != 1 && s != nil {
				log.Fatalf("Unexpected values for i(%d)  or s(%s) ", *i, *s)
			}
		} else {
			if i != nil && *s != strconv.Itoa(j) {
				log.Fatalf("Unexpected values for i(%d)  or s(%s) ", *i, *s)
			}
		}
		j++
	}
	if cursor.hasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	if j != 6000 {
		t.Fatalf("6000 rows expected here")
	}
	closeAll(t, connection, cursor)
}

func NoopDialContext(ctx context.Context, network string, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

func sleepContext(ctx context.Context, delay time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

func SleepDialContext(ctx context.Context, network string, addr string) (net.Conn, error) {
	var d net.Dialer
	sleepContext(ctx, 10*time.Second)
	return d.DialContext(ctx, network, addr)
}

func TestSimpleSelectWithDialFunction(t *testing.T) {
	configuration := newConnectConfiguration()
	configuration.DialContext = NoopDialContext
	configuration.TransportMode = getTransport()
	configuration.Service = "hive"
	configuration.FetchSize = 1000

	connection, cursor := makeConnectionWithConnectConfiguration(t, configuration)
	tableName := createTable(t, cursor)
	insertInTableSingleValue(t, cursor, tableName, 1)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	var s string
	var i int32
	cursor.fetchOne(context.Background(), &i, &s)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	closeAll(t, connection, cursor)
}

func TestSimpleSelectWithDialFunctionAndTimeout(t *testing.T) {
	configuration := newConnectConfiguration()
	configuration.DialContext = NoopDialContext
	configuration.TransportMode = getTransport()
	configuration.Service = "hive"
	configuration.FetchSize = 1000
	configuration.ConnectTimeout = time.Hour

	connection, cursor := makeConnectionWithConnectConfiguration(t, configuration)
	tableName := createTable(t, cursor)
	insertInTableSingleValue(t, cursor, tableName, 1)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	var s string
	var i int32
	cursor.fetchOne(context.Background(), &i, &s)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	closeAll(t, connection, cursor)
}

func TestSimpleSelectWithTimeout(t *testing.T) {
	configuration := newConnectConfiguration()
	configuration.ConnectTimeout = time.Hour
	configuration.TransportMode = getTransport()
	configuration.Service = "hive"
	configuration.FetchSize = 1000

	connection, cursor := makeConnectionWithConnectConfiguration(t, configuration)
	tableName := createTable(t, cursor)
	insertInTableSingleValue(t, cursor, tableName, 1)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	var s string
	var i int32
	cursor.fetchOne(context.Background(), &i, &s)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	closeAll(t, connection, cursor)
}

func TestConnectTimeoutWithDialFn(t *testing.T) {
	mode := getTransport()
	ssl := getSsl()
	configuration := newConnectConfiguration()
	configuration.Service = "hive"
	configuration.TransportMode = mode
	configuration.ConnectTimeout = 3 * time.Second
	configuration.DialContext = SleepDialContext

	if ssl {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}

	var port int = 10000
	if mode == "http" {
		configuration.HTTPPath = "cliservice"
	}
	start := time.Now()
	connection, errConn := connect(context.Background(), "hs2.example.com", port, getAuth(), configuration)
	elapsed := time.Since(start)
	if errConn == nil {
		connection.close()
		t.Fatal("Error was expected because the target port is blocked")
	}
	if !strings.Contains(errConn.Error(), "timeout") {
		t.Fatalf("Expected a timeout error, but received: %+v", errConn)
	}
	if elapsed <= 2*time.Second {
		t.Fatalf("Timed out too fast: %v", elapsed)
	}
	if elapsed >= 4*time.Second {
		t.Fatalf("Timed out too slow: %v", elapsed)
	}
}

func TestConnectTimeout(t *testing.T) {
	mode := getTransport()
	ssl := getSsl()
	configuration := newConnectConfiguration()
	configuration.Service = "hive"
	configuration.TransportMode = mode
	configuration.ConnectTimeout = 3 * time.Second

	if ssl {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}

	var port int = 10001
	if mode == "http" {
		configuration.HTTPPath = "cliservice"
	}
	start := time.Now()
	connection, errConn := connect(context.Background(), "example.com", port, getAuth(), configuration)
	elapsed := time.Since(start)
	if errConn == nil {
		connection.close()
		t.Fatal("Error was expected because the target port is blocked")
	}
	if !strings.Contains(errConn.Error(), "timeout") {
		t.Fatalf("Expected a timeout error, but received: %+v", errConn)
	}
	if elapsed <= 2*time.Second {
		t.Fatalf("Timed out too fast: %v", elapsed)
	}
	if elapsed >= 4*time.Second {
		t.Fatalf("Timed out too slow: %v", elapsed)
	}
}

func TestSimpleSelectWithNil(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 0, 1000)
	cursor.exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (1, NULL) ", tableName))
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	var s string
	var i int32
	cursor.fetchOne(context.Background(), &i, &s)

	if i != 1 || s != "" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}

	closeAll(t, connection, cursor)
}

func TestIsRow(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 1, 1000)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	var i int32
	var s string

	cursor.fetchOne(context.Background(), &i, &s)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	if i != 1 || s != "1" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}
	if cursor.hasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	for i := 0; i < 10; i++ {
		cursor.fetchOne(context.Background(), &i, &s)
		if cursor.error() == nil {
			t.Fatal("Error shouldn't be nil")
		}
		if cursor.error().Error() != "No more rows are left" {
			t.Fatal("Error should be 'No more rows are left'")
		}
	}

	closeAll(t, connection, cursor)
}

func TestFetchContext(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 2, 1000)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	var i int32
	var s string

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
	defer cancel()
	time.Sleep(500 * time.Millisecond)
	cursor.fetchOne(ctx, &i, &s)

	if cursor.error() == nil {
		t.Fatal("Error should be context has been done")
	}
	closeAll(t, connection, cursor)
}

func TestFetchLogs(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 2, 1000)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	logs := cursor.fetchLogs()
	if logs == nil {
		t.Fatal("Logs should not be nil")
	}

	if len(logs) == 0 {
		t.Fatal("Logs should non-empty")
	}

	if cursor.error() != nil {
		t.Fatal("Error should be nil")
	}

	closeAll(t, connection, cursor)
}

func TestFetchLogsDuringExecution(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 2, 1000)
	// Buffered so we only have to read at end

	logs := make(chan []string, 30)
	defer close(logs)

	cursor.Logs = logs
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	if len(logs) == 0 {
		t.Fatal("Logs should be non-empty")
	}

	closeAll(t, connection, cursor)
}

func TestHiveError(t *testing.T) {
	connection, cursor, _ := prepareTable(t, 2, 1000)
	cursor.exec(context.Background(), "SELECT * FROM table_doesnt_exist")
	if cursor.error() == nil {
		t.Fatal("Querying a non-existing table should cause an error")
	}

	hiveErr, ok := cursor.error().(hiveError)
	if !ok {
		t.Fatal("A HiveError should have been returned")
	}

	// table not found is code 10001 (1xxxx is SemanticException)
	if hiveErr.ErrorCode != 10001 {
		t.Fatalf("expected error code 10001, got %d", hiveErr.ErrorCode)
	}
	if hiveErr.Message != "Error while compiling statement: FAILED: SemanticException [Error 10001]: Line 1:14 Table not found 'table_doesnt_exist'" {
		t.Fatalf("expected error message: 10001, got %s", hiveErr.Message)
	}

	closeAll(t, connection, cursor)
}

func TestHasMoreContext(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 2, 1)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	var i int32
	var s string

	cursor.fetchOne(context.Background(), &i, &s)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(0)*time.Millisecond)
	defer cancel()
	time.Sleep(500 * time.Millisecond)
	cursor.hasMore(ctx)
	if cursor.error() == nil {
		t.Fatal("Error should be context has been done")
	}
	closeAll(t, connection, cursor)
}

func TestRowMap(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 2, 1)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	m := cursor.rowMap(context.Background())
	expected := map[string]interface{}{fmt.Sprintf("%s.a", tableName): int32(1), fmt.Sprintf("%s.b", tableName): "1"}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	m = cursor.rowMap(context.Background())
	expected = map[string]interface{}{fmt.Sprintf("%s.a", tableName): int32(2), fmt.Sprintf("%s.b", tableName): "2"}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	if cursor.hasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}

	closeAll(t, connection, cursor)
}

func TestRowMapColumnRename(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	tableId++
	tableName := fmt.Sprintf("tableT%s%d", randName, tableId)
	cursor.exec(context.Background(), fmt.Sprintf("create table if not exists %s(a int, b int)", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	cursor.exec(context.Background(), fmt.Sprintf("insert into %s values(1,2)", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	cursor.exec(context.Background(), fmt.Sprintf("select * from %s as x left join %s as y on x.a=y.b", tableName, tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	m := cursor.rowMap(context.Background())
	expected := map[string]interface{}{"x.a": int32(1), "x.b": int32(2), "y.a": nil, "y.b": nil}
	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	if cursor.hasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}
	cursor.exec(context.Background(), fmt.Sprintf("drop table %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	closeAll(t, connection, cursor)
}

func TestRowMapAllTypes(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)

	cursor.exec(context.Background(), "SELECT * FROM all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	m := cursor.rowMap(context.Background())
	expected := map[string]interface{}{
		"all_types.smallint":  int16(32767),
		"all_types.int":       int32(2147483647),
		"all_types.float":     float64(0.5),
		"all_types.double":    float64(0.25),
		"all_types.string":    "a string",
		"all_types.boolean":   true,
		"all_types.struct":    "{\"a\":1,\"b\":2}",
		"all_types.bigint":    int64(9223372036854775807),
		"all_types.array":     "[1,2]",
		"all_types.map":       "{1:2,3:4}",
		"all_types.decimal":   "0.1",
		"all_types.binary":    []uint8{49, 50, 51},
		"all_types.timestamp": "1970-01-01 00:00:00",
		"all_types.union":     "{0:1}",
		"all_types.tinyint":   int8(127),
	}

	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	closeAll(t, connection, cursor)
}

func TestRowMapAllTypesWithNull(t *testing.T) {
	if os.Getenv("METASTORE_SKIP") != "1" {
		t.Skip("skipping test because the local metastore is not working correctly.")
	}
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTableWithNull(t, cursor)

	cursor.exec(context.Background(), "SELECT * FROM all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	m := cursor.rowMap(context.Background())
	expected := map[string]interface{}{
		"all_types.smallint":  nil,
		"all_types.int":       int32(2147483647),
		"all_types.float":     nil,
		"all_types.double":    nil,
		"all_types.string":    nil,
		"all_types.boolean":   nil,
		"all_types.struct":    nil,
		"all_types.bigint":    nil,
		"all_types.array":     nil,
		"all_types.map":       nil,
		"all_types.decimal":   nil,
		"all_types.binary":    nil,
		"all_types.timestamp": nil,
		"all_types.tinyint":   nil,
	}

	if !reflect.DeepEqual(m, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, m)
	}

	closeAll(t, connection, cursor)
}

func TestSmallFetchSize(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 4, 2)

	var i int32
	var s string
	var j int

	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	// Fetch all rows
	// The query happens behind the scenes
	// The other rows are discarted
	for j = 0; cursor.hasMore(context.Background()); {
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
		cursor.fetchOne(context.Background(), &i, &s)
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
		j++
	}
	if i != 4 || s != "4" {
		log.Fatalf("Unexpected values for i(%d)  or s(%s) ", i, s)
	}
	if cursor.hasMore(context.Background()) {
		log.Fatal("Shouldn't have any more values")
	}
	if j != 4 {
		t.Fatalf("Fetch size was set to 4 but had %d iterations", j)
	}

	closeAll(t, connection, cursor)
}

func TestWithContextSync(t *testing.T) {
	if os.Getenv("SKIP_UNSTABLE") == "1" {
		return
	}
	connection, cursor, tableName := prepareTable(t, 0, 1000)

	values := []int{0, 0, 0, 200, 200, 200, 300, 400, 100, 500, 1000}

	for _, value := range values {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(value)*time.Millisecond)
		defer cancel()
		cursor.exec(ctx, fmt.Sprintf("SELECT reflect('java.lang.Thread', 'sleep', 1000L * 1000L) FROM %s a JOIN %s b", tableName, tableName))
		if cursor.error() == nil {
			t.Fatal("Error should be context has been done")
		}
	}

	closeAll(t, connection, cursor)
}

func TestWithContextAsync(t *testing.T) {
	if os.Getenv("SKIP_UNSTABLE") == "1" {
		return
	}
	connection, cursor, tableName := prepareTable(t, 0, 1000)

	value := 0

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(value)*time.Millisecond)
		defer cancel()
		time.Sleep(100 * time.Millisecond)
		cursor.exec(ctx, fmt.Sprintf("SELECT reflect('java.lang.Thread', 'sleep', 1000L * 1000L) FROM %s a JOIN %s b", tableName, tableName))
		if cursor.error() == nil {
			t.Fatal("Error shouldn't happen despite the context being done: ", cursor.error())
		}
	}

	closeAll(t, connection, cursor)
}

func TestNoResult(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 0, 1000)

	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	if cursor.hasMore(context.Background()) {
		t.Fatal("Shouldn't have any rows")
	}

	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	closeAll(t, connection, cursor)
}

func TestHasMore(t *testing.T) {
	connection, cursor, tableName := prepareTable(t, 5, 1000)
	cursor.exec(context.Background(), fmt.Sprintf("SELECT * FROM %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	for i := 0; i < 10; i++ {
		if !cursor.hasMore(context.Background()) {
			t.Fatalf("Should have more rows, iteration %d", i)
		}
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
	}

	var j int32
	var s string
	for i := 0; i < 5; i++ {
		if !cursor.hasMore(context.Background()) {
			t.Fatalf("Should have more rows, iteration %d", i)
		}
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
		cursor.fetchOne(context.Background(), &j, &s)
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
	}
	if cursor.hasMore(context.Background()) {
		t.Fatalf("Should not have more rows")
	}
	closeAll(t, connection, cursor)
}

func TestTypesError(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)

	var b bool
	var tinyInt int8
	var smallInt int16
	var normalInt int32
	var bigInt int64
	// This value is store as a float32. The go thrift API returns a floa64 though.
	var floatType float64
	var double float64
	var s string
	var timeStamp string
	var binary []byte
	var array string
	var mapType string
	var structType string
	var union string
	var decimal string
	var dummy chan<- int

	cursor.exec(context.Background(), "SELECT * FROM all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &dummy, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &dummy, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &dummy, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &dummy, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &dummy,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&dummy, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &dummy, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &dummy, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &dummy, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &dummy, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &dummy, &mapType, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &dummy, &structType, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &dummy, &union, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &dummy, &decimal)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &dummy)
	if cursor.error() == nil {
		t.Fatal("Error should have happened because there are not enough arguments")
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	d := cursor.description(context.Background())
	expected := [][]string{
		[]string{"all_types.boolean", "BOOLEAN_TYPE"},
		[]string{"all_types.tinyint", "TINYINT_TYPE"},
		[]string{"all_types.smallint", "SMALLINT_TYPE"},
		[]string{"all_types.int", "INT_TYPE"},
		[]string{"all_types.bigint", "BIGINT_TYPE"},
		[]string{"all_types.float", "FLOAT_TYPE"},
		[]string{"all_types.double", "DOUBLE_TYPE"},
		[]string{"all_types.string", "STRING_TYPE"},
		[]string{"all_types.timestamp", "TIMESTAMP_TYPE"},
		[]string{"all_types.binary", "BINARY_TYPE"},
		[]string{"all_types.array", "ARRAY_TYPE"},
		[]string{"all_types.map", "MAP_TYPE"},
		[]string{"all_types.struct", "STRUCT_TYPE"},
		[]string{"all_types.union", "UNION_TYPE"},
		[]string{"all_types.decimal", "DECIMAL_TYPE"},
	}
	if !reflect.DeepEqual(d, expected) {
		t.Fatalf("Expected map: %+v, got: %+v", expected, d)
	}

	closeAll(t, connection, cursor)
}

func TestTypes(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)
	var b bool
	var tinyInt int8
	var smallInt int16
	var normalInt int32
	var bigInt int64
	// This value is store as a float32. The go thrift API returns a floa64 though.
	var floatType float64
	var double float64
	var s string
	var timeStamp string
	var binary []byte
	var array string
	var mapType string
	var structType string
	var union string
	var decimal string

	cursor.exec(context.Background(), "SELECT * FROM all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	closeAll(t, connection, cursor)
}

func TestTypesInterface(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)

	cursor.exec(context.Background(), "SELECT * FROM all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	i := make([]interface{}, 15)
	expected := make([]interface{}, 15)
	expected[0] = true
	expected[1] = int8(127)
	expected[2] = int16(32767)
	expected[3] = int32(2147483647)
	expected[4] = int64(9223372036854775807)
	expected[5] = float64(0.5)
	expected[6] = float64(0.25)
	expected[7] = "a string"
	expected[8] = "1970-01-01 00:00:00"
	expected[9] = []uint8{49, 50, 51}
	expected[10] = "[1,2]"
	expected[11] = "{1:2,3:4}"
	expected[12] = "{\"a\":1,\"b\":2}"
	expected[13] = "{0:1}"
	expected[14] = "0.1"

	cursor.fetchOne(context.Background(), i...)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	if !reflect.DeepEqual(i, expected) {
		t.Fatalf("Expected array: %+v, got: %+v", expected, i)
	}

	closeAll(t, connection, cursor)
}

func TestTypesWithPointer(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)
	var b bool
	var tinyInt *int8 = new(int8)
	var smallInt *int16 = new(int16)
	var normalInt *int32 = new(int32)
	var bigInt *int64 = new(int64)
	var floatType *float64 = new(float64)
	var double *float64 = new(float64)
	var s *string = new(string)
	var timeStamp *string = new(string)
	var binary []byte
	var array *string = new(string)
	var mapType *string = new(string)
	var structType *string = new(string)
	var union *string = new(string)
	var decimal *string = new(string)

	cursor.exec(context.Background(), "SELECT * FROM all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	if *tinyInt != 127 || *smallInt != 32767 || *bigInt != 9223372036854775807 || binary == nil || *array != "[1,2]" || *s != "a string" {
		t.Fatalf("Unexpected value, tinyInt: %d, smallInt: %d, bigInt: %d, binary: %x, array: %s, s: %s", *tinyInt, *smallInt, *bigInt, binary, *array, *s)
	}

	closeAll(t, connection, cursor)
}

func TestTypesWithoutInitializedPointer(t *testing.T) {
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTable(t, cursor)
	var b bool
	var tinyInt *int8
	var smallInt *int16
	var normalInt *int32
	var bigInt *int64
	var floatType *float64
	var double *float64
	var s *string
	var timeStamp *string
	var binary []byte
	var array *string
	var mapType *string
	var structType *string
	var union *string
	var decimal *string

	cursor.exec(context.Background(), "SELECT * FROM all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &union, &decimal)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	if *tinyInt != 127 || *smallInt != 32767 || *bigInt != 9223372036854775807 || binary == nil || *array != "[1,2]" || *s != "a string" {
		t.Fatalf("Unexpected value, tinyInt: %d, smallInt: %d, bigInt: %d, binary: %x, array: %s, s: %s", *tinyInt, *smallInt, *bigInt, binary, *array, *s)
	}

	closeAll(t, connection, cursor)
}

func TestTypesWithNulls(t *testing.T) {
	if os.Getenv("METASTORE_SKIP") != "1" {
		t.Skip("skipping test because the local metastore is not working correctly.")
	}
	connection, cursor := makeConnection(t, 1000)
	prepareAllTypesTableWithNull(t, cursor)
	var b bool
	var tinyInt *int8 = new(int8)
	var smallInt *int16 = new(int16)
	var normalInt *int32 = new(int32)
	var bigInt *int64 = new(int64)
	// This value is store as a float32. The go thrift API returns a floa64 though.
	var floatType *float64 = new(float64)
	var double *float64 = new(float64)
	var s *string = new(string)
	var timeStamp string
	var binary []byte
	var array string
	var mapType string
	var structType string
	var decimal string

	cursor.exec(context.Background(), "SELECT * FROM all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	cursor.fetchOne(context.Background(), &b, &tinyInt, &smallInt, &normalInt, &bigInt,
		&floatType, &double, &s, &timeStamp, &binary, &array, &mapType, &structType, &decimal)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	if tinyInt != nil || smallInt != nil || bigInt != nil || binary != nil || array != "" || s != nil {
		t.Fatalf("Unexpected value, tinyInt: %p, smallInt: %p, bigInt: %p, binary: %x, array: %s, s: %s", tinyInt, smallInt, bigInt, binary, array, *s)
	}

	closeAll(t, connection, cursor)
}

func prepareAllTypesTable(t *testing.T, cursor *cursor) {
	createAllTypesTable(t, cursor)
	insertAllTypesTable(t, cursor)
}

func prepareAllTypesTableWithNull(t *testing.T, cursor *cursor) {
	createAllTypesTableNoUnion(t, cursor)
	insertAllTypesTableWithNulls(t, cursor)
}

func createAllTypesTable(t *testing.T, cursor *cursor) {
	cursor.exec(context.Background(), "DROP TABLE IF EXISTS all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	createAll := "CREATE TABLE all_types (" +
		"`boolean` BOOLEAN," +
		"`tinyint` TINYINT," +
		"`smallint` SMALLINT," +
		"`int` INT," +
		"`bigint` BIGINT," +
		"`float` FLOAT," +
		"`double` DOUBLE," +
		"`string` STRING," +
		"`timestamp` TIMESTAMP," +
		"`binary` BINARY," +
		"`array` ARRAY<int>," +
		"`map` MAP<int, int>," +
		"`struct` STRUCT<a: int, b: int>," +
		"`union` UNIONTYPE<int, string>," +
		"`decimal` DECIMAL(10, 1))"
	cursor.exec(context.Background(), createAll)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
}

func createAllTypesTableNoUnion(t *testing.T, cursor *cursor) {
	cursor.exec(context.Background(), "DROP TABLE IF EXISTS all_types")
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}

	createAll := "CREATE TABLE all_types (" +
		"`boolean` BOOLEAN," +
		"`tinyint` TINYINT," +
		"`smallint` SMALLINT," +
		"`int` INT," +
		"`bigint` BIGINT," +
		"`float` FLOAT," +
		"`double` DOUBLE," +
		"`string` STRING," +
		"`timestamp` TIMESTAMP," +
		"`binary` BINARY," +
		"`array` ARRAY<int>," +
		"`map` MAP<int, int>," +
		"`struct` STRUCT<a: int, b: int>," +
		"`decimal` DECIMAL(10, 1))"
	cursor.exec(context.Background(), createAll)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
}

func insertAllTypesTable(t *testing.T, cursor *cursor) {
	insertAll := `INSERT INTO TABLE all_types VALUES(
		true,
		127,
		32767,
		2147483647,
		9223372036854775807,
		0.5,
		0.25,
		'a string',
		0,
		'123',
		array(1, 2),
		map(1, 2, 3, 4),
		named_struct('a', 1, 'b', 2),
		create_union(0, 1, 'test_string'),
		0.1)`
	cursor.exec(context.Background(), insertAll)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
}

func insertAllTypesTableWithNulls(t *testing.T, cursor *cursor) {
	insertAll := "INSERT INTO TABLE all_types(`int`) VALUES(2147483647)"
	cursor.exec(context.Background(), insertAll)
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
}

func prepareTable(t *testing.T, rowsToInsert int, fetchSize int64) (*connection, *cursor, string) {
	connection, cursor := makeConnection(t, fetchSize)
	tableName := createTable(t, cursor)
	insertInTable(t, cursor, tableName, rowsToInsert)
	return connection, cursor, tableName
}

func prepareTableSingleValue(t *testing.T, rowsToInsert int, fetchSize int64) (*connection, *cursor, string) {
	connection, cursor := makeConnection(t, fetchSize)
	tableName := createTable(t, cursor)
	insertInTableSingleValue(t, cursor, tableName, rowsToInsert)
	return connection, cursor, tableName
}

func createTable(t *testing.T, cursor *cursor) string {
	tableId++
	tableName := fmt.Sprintf("pokes_%s%d", randName, tableId)
	cursor.exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	cursor.exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (a INT, b STRING)", tableName))
	if cursor.error() != nil {
		t.Fatal(cursor.error())
	}
	return tableName
}

func insertInTable(t *testing.T, cursor *cursor, tableName string, rowsToInsert int) {
	if rowsToInsert > 0 {
		values := ""
		for i := 1; i <= rowsToInsert; i++ {
			values += fmt.Sprintf("(%d, '%d')", i, i)
			if i != rowsToInsert {
				values += ","
			}
		}
		cursor.exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES ", tableName)+values)
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
	}
}

func insertInTableSingleValue(t *testing.T, cursor *cursor, tableName string, rowsToInsert int) {
	if rowsToInsert > 0 {
		values := ""
		for i := 1; i <= rowsToInsert; i++ {
			values += fmt.Sprintf("('%d')", i)
			if i != rowsToInsert {
				values += ","
			}
		}
		cursor.exec(context.Background(), fmt.Sprintf("INSERT INTO %s(b) VALUES ", tableName)+values)
		if cursor.error() != nil {
			t.Fatal(cursor.error())
		}
	}
}

func makeConnection(t *testing.T, fetchSize int64) (*connection, *cursor) {
	return makeConnectionWithConfiguration(t, fetchSize, nil)
}

func makeConnectionWithConfiguration(t *testing.T, fetchSize int64, hiveConfiguration map[string]string) (*connection, *cursor) {
	mode := getTransport()
	ssl := getSsl()
	configuration := newConnectConfiguration()
	configuration.Service = "hive"
	configuration.FetchSize = fetchSize
	configuration.TransportMode = mode
	configuration.HiveConfiguration = hiveConfiguration
	configuration.MaxSize = 16384001

	if ssl {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}

	var port int = 10000
	if mode == "http" {
		port = 10000
		configuration.HTTPPath = "cliservice"
	}
	connection, errConn := connect(context.Background(), "hs2.example.com", port, getAuth(), configuration)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.cursor()
	return connection, cursor
}

func makeConnectionWithConnectConfiguration(t *testing.T, configuration *connectConfiguration) (*connection, *cursor) {
	mode := getTransport()
	ssl := getSsl()
	if ssl {
		tlsConfig, err := getTlsConfiguration("client.cer.pem", "client.cer.key")
		configuration.TLSConfig = tlsConfig
		if err != nil {
			t.Fatal(err)
		}
	}

	var port int = 10000
	if mode == "http" {
		port = 10000
		configuration.HTTPPath = "cliservice"
	}
	connection, errConn := connect(context.Background(), "hs2.example.com", port, getAuth(), configuration)
	if errConn != nil {
		t.Fatal(errConn)
	}
	cursor := connection.cursor()
	return connection, cursor
}

func closeAll(t *testing.T, connection *connection, cursor *cursor) {
	if cursor != nil {
		cursor.close(context.Background())
	}
	if connection != nil {
		connection.close()
	}
}

func getAuth() string {
	auth := os.Getenv("AUTH")
	os.Setenv("KRB5CCNAME", "/tmp/krb5_gohive")
	if auth == "" {
		auth = "KERBEROS"
	}
	return auth
}

func getTransport() string {
	transport := os.Getenv("TRANSPORT")
	if transport == "" {
		transport = "binary"
	}
	return transport
}

func getSsl() bool {
	ssl := os.Getenv("SSL")
	if ssl == "1" {
		return true
	}
	return false
}
