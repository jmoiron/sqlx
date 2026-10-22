package sqlx

import (
	"net/url"
	"regexp"
	"strings"
)

var kvPasswordRegex = regexp.MustCompile(`(?i)\b(password|pwd)\s*=\s*('[^']*'|"[^"]*"|[^\s;&]*)`)
var userPassRegex = regexp.MustCompile(`\b([a-zA-Z0-9_\.\-%]+):([^@/\\"'` + "`" + `\s]+)(@[a-zA-Z0-9_\.\-%/()]+)`)

type unwrapper interface {
	Unwrap() error
}

func unwrapError(err error) error {
	u, ok := err.(unwrapper)
	if !ok {
		return nil
	}
	return u.Unwrap()
}

func findURLError(err error) *url.Error {
	for e := err; e != nil; {
		if urlErr, ok := e.(*url.Error); ok {
			return urlErr
		}
		e = unwrapError(e)
	}
	return nil
}

type redactedError struct {
	err error
	msg string
}

func (e *redactedError) Error() string {
	return e.msg
}

func (e *redactedError) Unwrap() error {
	return e.err
}

// RedactURL redacts credentials from a database URL or connection string.
// If userinfo contains a password (e.g. scheme://user:pass@host), the password
// is replaced with "xxxxx". Malformed URLs and invalid userinfo where url.Parse fails
// (such as spaces or unescaped characters) are safely parsed and redacted.
func RedactURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}

	res := rawURL

	// 1. Check for scheme delimiter "://" or authority prefix "//"
	schemeIdx := strings.Index(res, "://")
	var schemeEnd int
	if schemeIdx != -1 {
		schemeEnd = schemeIdx + 3
	} else if strings.HasPrefix(res, "//") {
		schemeEnd = 2
	}

	if schemeEnd > 0 {
		rest := res[schemeEnd:]
		// Find end of authority (first '/', '?', '#', or end of string)
		authEnd := len(rest)
		for i := 0; i < len(rest); i++ {
			c := rest[i]
			if c == '/' || c == '?' || c == '#' {
				authEnd = i
				break
			}
		}
		authority := rest[:authEnd]
		remainder := rest[authEnd:]

		// In authority, userinfo ends at the last '@'
		atIdx := strings.LastIndex(authority, "@")
		if atIdx != -1 {
			userinfo := authority[:atIdx]
			hostEtc := authority[atIdx:] // includes '@'
			colonIdx := strings.Index(userinfo, ":")
			if colonIdx != -1 {
				username := userinfo[:colonIdx]
				authority = username + ":xxxxx" + hostEtc
				res = res[:schemeEnd] + authority + remainder
			}
		}
	} else {
		// 2. Check for MySQL-style DSN without scheme: [user[:password]@]address/dbname
		atIdx := strings.LastIndex(res, "@")
		if atIdx != -1 {
			userinfo := res[:atIdx]
			// Ensure userinfo does not span paths or query parameters
			if !strings.ContainsAny(userinfo, "/\\?#") {
				colonIdx := strings.Index(userinfo, ":")
				if colonIdx != -1 {
					username := userinfo[:colonIdx]
					res = username + ":xxxxx" + res[atIdx:]
				}
			}
		}
	}

	// 3. Redact any key-value passwords (e.g. in DSNs or query strings: password=... or pwd=...)
	res = kvPasswordRegex.ReplaceAllString(res, "${1}=xxxxx")

	return res
}

// RedactDSN redacts credentials from a database URL or DSN connection string.
func RedactDSN(dsn string) string {
	return RedactURL(dsn)
}

// RedactString redacts any credentials in URLs or DSNs found within an arbitrary string.
func RedactString(s string) string {
	if s == "" {
		return ""
	}

	// 1. Scan for URLs with "://" and redact them
	searchFrom := 0
	for {
		idx := strings.Index(s[searchFrom:], "://")
		if idx == -1 {
			break
		}
		schemeSep := searchFrom + idx

		// Scan backwards to find the start of the URL (scheme)
		start := schemeSep
		for start > 0 {
			r := s[start-1]
			if r == '"' || r == '\'' || r == '`' || r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '(' || r == '<' || r == '[' {
				break
			}
			start--
		}

		// Determine the end of the URL
		end := schemeSep + 3
		if start > 0 && (s[start-1] == '"' || s[start-1] == '\'' || s[start-1] == '`') {
			quote := s[start-1]
			for end < len(s) && s[end] != quote {
				end++
			}
		} else {
			for end < len(s) {
				r := s[end]
				if r == '"' || r == '\'' || r == '`' || r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == ')' || r == '>' || r == ']' {
					break
				}
				end++
			}
		}

		urlCandidate := s[start:end]
		redacted := RedactURL(urlCandidate)
		if redacted != urlCandidate {
			s = s[:start] + redacted + s[end:]
			searchFrom = start + len(redacted)
		} else {
			searchFrom = end
		}
	}

	// 2. Redact MySQL-style user:pass@ patterns where user and pass do not contain slashes or quotes
	s = userPassRegex.ReplaceAllString(s, "${1}:xxxxx${3}")

	// 3. Redact key-value passwords
	s = kvPasswordRegex.ReplaceAllString(s, "${1}=xxxxx")

	return s
}

// RedactError returns an error with any credentials redacted from URLs or DSNs.
// If err is nil, it returns nil.
func RedactError(err error) error {
	if err == nil {
		return nil
	}

	// If err is or wraps a *url.Error, redact the URL field in place.
	if urlErr := findURLError(err); urlErr != nil {
		urlErr.URL = RedactURL(urlErr.URL)
	}

	origMsg := err.Error()
	redactedMsg := RedactString(origMsg)
	if redactedMsg == origMsg {
		return err
	}

	return &redactedError{
		err: err,
		msg: redactedMsg,
	}
}

func redactError(err error) error {
	return RedactError(err)
}
