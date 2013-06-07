package sqlx

import (
	"bytes"
	"errors"
	"strconv"
	"unicode"
)

const (
	UNKNOWN = iota
	QUESTION
	DOLLAR
)

func bindType(driverName string) int {
	switch driverName {
	case "postgres":
		return DOLLAR
	case "mysql":
		return QUESTION
	case "sqlite":
		return QUESTION
	}
	return UNKNOWN
}

func rebind(bindType int, query string) string {
	if bindType == QUESTION {
		return query
	}

	qb := []byte(query)
	// Add space enough for 10 params before we have to allocate
	rqb := make([]byte, 0, len(qb)+10)
	j := 1
	for _, b := range qb {
		if b == '?' {
			rqb = append(rqb, '$')
			for _, b := range strconv.Itoa(j) {
				rqb = append(rqb, byte(b))
			}
			j++
		} else {
			rqb = append(rqb, b)
		}
	}

	return string(rqb)
}

// Bind named parameters to a query string and list of arguments, in order.
// If the
func bindMap(query string, args map[string]interface{}) (string, []interface{}, error) {
	arglist := make([]interface{}, 0, 5)
	// In all likelihood, the rebound query will be shorter
	qb := []byte(query)
	rebound := make([]byte, 0, len(qb))

	var name []byte
	inName := false

	for i, b := range qb {
		if b == ':' {
			if inName {
				err := errors.New("Unexpected `:` while reading named param at " + strconv.Itoa(i))
				return "", arglist, err
			}
			inName = true
			name = []byte{}
		} else if inName && unicode.IsLetter(rune(b)) {
			name = append(name, b)
		} else if inName {
			inName = false
			sname := string(name)
			val, ok := args[sname]
			if !ok {
				err := errors.New("Could not find name `" + sname + "` in args")
				return "", arglist, err
			}
			arglist = append(arglist, val)
			rebound = append(rebound, '?')
			rebound = append(rebound, b)
		} else {
			rebound = append(rebound, b)
		}
	}
	return string(rebound), arglist, nil
}

func rebindBuff(bindType int, query string) string {
	if bindType == QUESTION {
		return query
	}

	b := make([]byte, 0, len(query))
	rqb := bytes.NewBuffer(b)
	j := 1
	for _, r := range query {
		if r == '?' {
			rqb.WriteString("$" + strconv.Itoa(j))
			j++
		} else {
			rqb.WriteRune(r)
		}
	}

	return rqb.String()
}
