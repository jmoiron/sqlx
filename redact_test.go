package sqlx

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

type customWrappedError struct {
	err error
	msg string
}

func (e *customWrappedError) Error() string {
	return fmt.Sprintf("%s: %v", e.msg, e.err)
}

func (e *customWrappedError) Unwrap() error {
	return e.err
}

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty URL",
			input:    "",
			expected: "",
		},
		{
			name:     "valid URL with user and password",
			input:    "postgres://user:secret@localhost:5432/mydb?sslmode=disable",
			expected: "postgres://user:xxxxx@localhost:5432/mydb?sslmode=disable",
		},
		{
			name:     "space before password (issue 954)",
			input:    "postgres://dummy_user: mysecret@dummywebsite.com:12241/dummy_db",
			expected: "postgres://dummy_user:xxxxx@dummywebsite.com:12241/dummy_db",
		},
		{
			name:     "space inside password",
			input:    "postgres://user:my secret pass@localhost:5432/db",
			expected: "postgres://user:xxxxx@localhost:5432/db",
		},
		{
			name:     "space before and after colon",
			input:    "postgres://user : pass@localhost:5432/db",
			expected: "postgres://user :xxxxx@localhost:5432/db",
		},
		{
			name:     "empty password with colon",
			input:    "postgres://user:@localhost:5432/db",
			expected: "postgres://user:xxxxx@localhost:5432/db",
		},
		{
			name:     "no username, only password",
			input:    "redis://:mypassword@localhost:6379",
			expected: "redis://:xxxxx@localhost:6379",
		},
		{
			name:     "username only, no password",
			input:    "postgres://username@localhost:5432/db",
			expected: "postgres://username@localhost:5432/db",
		},
		{
			name:     "no userinfo in URL",
			input:    "postgres://localhost:5432/db",
			expected: "postgres://localhost:5432/db",
		},
		{
			name:     "URL with IPv6 host and password",
			input:    "postgres://user:secret@[::1]:5432/db",
			expected: "postgres://user:xxxxx@[::1]:5432/db",
		},
		{
			name:     "URL with IPv6 host without credentials",
			input:    "postgres://[::1]:5432/db",
			expected: "postgres://[::1]:5432/db",
		},
		{
			name:     "multiple colons in password",
			input:    "postgres://user:p:a:s:s@localhost:5432/db",
			expected: "postgres://user:xxxxx@localhost:5432/db",
		},
		{
			name:     "multiple at signs in password",
			input:    "postgres://user:p@ss@word@localhost:5432/db",
			expected: "postgres://user:xxxxx@localhost:5432/db",
		},
		{
			name:     "invalid URL escape in userinfo",
			input:    "postgres://user:p%zz@localhost:5432/db",
			expected: "postgres://user:xxxxx@localhost:5432/db",
		},
		{
			name:     "tabs in password",
			input:    "postgres://user:p\tword@localhost:5432/db",
			expected: "postgres://user:xxxxx@localhost:5432/db",
		},
		{
			name:     "query parameter password",
			input:    "postgres://user:pass@localhost:5432/db?password=othersecret&sslmode=disable",
			expected: "postgres://user:xxxxx@localhost:5432/db?password=xxxxx&sslmode=disable",
		},
		{
			name:     "query parameter pwd",
			input:    "sqlserver://host/db?pwd=mysecret&user=sa",
			expected: "sqlserver://host/db?pwd=xxxxx&user=sa",
		},
		{
			name:     "MySQL DSN format",
			input:    "myuser:mypassword@tcp(127.0.0.1:3306)/dbname",
			expected: "myuser:xxxxx@tcp(127.0.0.1:3306)/dbname",
		},
		{
			name:     "MySQL DSN with space in password",
			input:    "myuser: my password@unix(/var/run/mysql.sock)/dbname",
			expected: "myuser:xxxxx@unix(/var/run/mysql.sock)/dbname",
		},
		{
			name:     "key-value Postgres DSN",
			input:    "host=localhost port=5432 user=myuser password=mysecret dbname=mydb",
			expected: "host=localhost port=5432 user=myuser password=xxxxx dbname=mydb",
		},
		{
			name:     "key-value DSN single quoted password",
			input:    "host=localhost user=myuser password='my secret' dbname=mydb",
			expected: "host=localhost user=myuser password=xxxxx dbname=mydb",
		},
		{
			name:     "key-value DSN double quoted password",
			input:    `host=localhost user=myuser password="my secret" dbname=mydb`,
			expected: `host=localhost user=myuser password=xxxxx dbname=mydb`,
		},
		{
			name:     "ODBC style DSN",
			input:    "Server=localhost;Database=mydb;User Id=usr;Password=secret;",
			expected: "Server=localhost;Database=mydb;User Id=usr;Password=xxxxx;",
		},
		{
			name:     "sqlite file URL without credentials",
			input:    "file:test.db?cache=shared&mode=memory",
			expected: "file:test.db?cache=shared&mode=memory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := RedactURL(tt.input)
			if actual != tt.expected {
				t.Errorf("RedactURL(%q) = %q, want %q", tt.input, actual, tt.expected)
			}
			dsnActual := RedactDSN(tt.input)
			if dsnActual != tt.expected {
				t.Errorf("RedactDSN(%q) = %q, want %q", tt.input, dsnActual, tt.expected)
			}
		})
	}
}

func TestRedactString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "quoted net/url Error with invalid userinfo space (issue 954)",
			input:    `parse "postgres://dummy_user: mysecret@dummywebsite.com:12241/dummy_db": net/url: invalid userinfo`,
			expected: `parse "postgres://dummy_user:xxxxx@dummywebsite.com:12241/dummy_db": net/url: invalid userinfo`,
		},
		{
			name:     "single-quoted URL error message",
			input:    `failed to parse 'postgres://dummy_user:secret@localhost:5432/db': connection refused`,
			expected: `failed to parse 'postgres://dummy_user:xxxxx@localhost:5432/db': connection refused`,
		},
		{
			name:     "unquoted URL error message",
			input:    `dial error connecting to postgres://user:secret@localhost:5432/db connection refused`,
			expected: `dial error connecting to postgres://user:xxxxx@localhost:5432/db connection refused`,
		},
		{
			name:     "key-value password in error message",
			input:    `driver error: failed with password=topsecret for user admin`,
			expected: `driver error: failed with password=xxxxx for user admin`,
		},
		{
			name:     "error message with no sensitive credentials",
			input:    `driver error: table not found: users`,
			expected: `driver error: table not found: users`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := RedactString(tt.input)
			if actual != tt.expected {
				t.Errorf("RedactString(%q) = %q, want %q", tt.input, actual, tt.expected)
			}
		})
	}
}

func TestRedactError(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		if err := RedactError(nil); err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})

	t.Run("direct net/url Error", func(t *testing.T) {
		raw := "postgres://dummy_user: mysecret@dummywebsite.com:12241/dummy_db"
		urlErr := &url.Error{
			Op:  "parse",
			URL: raw,
			Err: errors.New("net/url: invalid userinfo"),
		}
		redacted := RedactError(urlErr)
		if redacted == nil {
			t.Fatal("expected non-nil error")
		}

		errMsg := redacted.Error()
		if strings.Contains(errMsg, "mysecret") {
			t.Errorf("error message leaked secret: %s", errMsg)
		}
		if !strings.Contains(errMsg, "dummy_user:xxxxx@dummywebsite.com:12241/dummy_db") {
			t.Errorf("expected redacted userinfo in error, got: %s", errMsg)
		}

		var targetURLErr *url.Error
		if !errors.As(redacted, &targetURLErr) {
			t.Errorf("expected error to be unwrappable as *url.Error")
		} else if strings.Contains(targetURLErr.URL, "mysecret") {
			t.Errorf("targetURLErr.URL contains secret: %s", targetURLErr.URL)
		}
	})

	t.Run("wrapped net/url Error", func(t *testing.T) {
		raw := "postgres://dummy_user: mysecret@dummywebsite.com:12241/dummy_db"
		urlErr := &url.Error{
			Op:  "parse",
			URL: raw,
			Err: errors.New("net/url: invalid userinfo"),
		}
		wrapped := &customWrappedError{
			err: urlErr,
			msg: "failed to initialize db connection",
		}

		redacted := RedactError(wrapped)
		if redacted == nil {
			t.Fatal("expected non-nil error")
		}

		errMsg := redacted.Error()
		if strings.Contains(errMsg, "mysecret") {
			t.Errorf("error message leaked secret: %s", errMsg)
		}
		if !strings.Contains(errMsg, "dummy_user:xxxxx@dummywebsite.com:12241/dummy_db") {
			t.Errorf("expected redacted userinfo in error, got: %s", errMsg)
		}
	})

	t.Run("string error with DSN password", func(t *testing.T) {
		rawErr := errors.New("connect failed: host=localhost password=supersecret dbname=test")
		redacted := RedactError(rawErr)
		if redacted == nil {
			t.Fatal("expected non-nil error")
		}

		errMsg := redacted.Error()
		if strings.Contains(errMsg, "supersecret") {
			t.Errorf("error message leaked password: %s", errMsg)
		}
		if !strings.Contains(errMsg, "password=xxxxx") {
			t.Errorf("expected redacted password in error, got: %s", errMsg)
		}
	})
}

func TestConnectRedactsCredentials(t *testing.T) {
	invalidURL := "postgres://dummy_user: mysecret@dummywebsite.com:12241/dummy_db"

	db, err := Connect("postgres", invalidURL)
	if err == nil {
		t.Fatal("expected error connecting with invalid userinfo, got nil")
	}
	if db != nil {
		t.Errorf("expected nil DB on connection failure, got: %v", db)
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, "mysecret") {
		t.Errorf("Connect() leaked password in error message: %s", errMsg)
	}
	if !strings.Contains(errMsg, "dummy_user:xxxxx@dummywebsite.com:12241/dummy_db") {
		t.Errorf("Connect() did not redact userinfo properly: %s", errMsg)
	}
}

func TestConnectContextRedactsCredentials(t *testing.T) {
	invalidURL := "postgres://dummy_user: mysecret@dummywebsite.com:12241/dummy_db"

	ctx := context.Background()
	_, err := ConnectContext(ctx, "postgres", invalidURL)
	if err == nil {
		t.Fatal("expected error connecting with invalid userinfo, got nil")
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, "mysecret") {
		t.Errorf("ConnectContext() leaked password in error message: %s", errMsg)
	}
	if !strings.Contains(errMsg, "dummy_user:xxxxx@dummywebsite.com:12241/dummy_db") {
		t.Errorf("ConnectContext() did not redact userinfo properly: %s", errMsg)
	}
}

func TestMustConnectPanicsWithRedactedCredentials(t *testing.T) {
	invalidURL := "postgres://dummy_user: mysecret@dummywebsite.com:12241/dummy_db"

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected MustConnect to panic, did not panic")
		}
		var panicMsg string
		switch v := r.(type) {
		case error:
			panicMsg = v.Error()
		case string:
			panicMsg = v
		default:
			panicMsg = fmt.Sprint(v)
		}

		if strings.Contains(panicMsg, "mysecret") {
			t.Errorf("MustConnect() panic leaked password: %s", panicMsg)
		}
		if !strings.Contains(panicMsg, "dummy_user:xxxxx@dummywebsite.com:12241/dummy_db") {
			t.Errorf("MustConnect() panic did not redact userinfo: %s", panicMsg)
		}
	}()

	MustConnect("postgres", invalidURL)
}
