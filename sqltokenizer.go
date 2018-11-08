package sqlx

// This file implements an overly simplified SQL tokenizer (or lexer). It
// only recognizes and emits the following tokens:
//
//     Space:             runs of whitespace characters
//     Comment:           '--' comment line
//     BlockComment:      /* */ block comment (nests!)
//     Identifier:        any identifier (keywords too)
//     QuotedIdentifier:  quoted (") identifier
//     StringLiteral:     quoted (') string
//     Numeric:           a numeric constant
//     Operator:          of which parenthesis, comma, ...
//
// The syntax rules come from the PostgreSQL v10 documentation:
// https://www.postgresql.org/docs/10/static/sql-syntax-lexical.html
// However no PostgreSQL specific syntax elements are used. This lexer
// tries to be as close to sql-2003 as possible.
//
// Here is the list of lexical rules that are implemented in this file:
//
//     - An Identifier is a run of non-space characters, which matches
//       non of the other tokens.
//     - An Identifier token always ends upon encountering one of the
//       following characters (which is not included):
//       ' " ( ) [ ] , ; $ : . + - * / < > = ~ ! @ # % ^ & | ` ?
//   !!! SPECIAL RULE for sqlx: '.' is a valid identifier character used
//       when binding struct parameters.
//     - An operator token is one of the following (order of priority):
//       ??( ??)
//       <= <> >= || :: .. -> !=
//       ( ) [ ] , ; . + - ^ * / % < > =
//     - A period (.) followed by a digit is the start of a Numeric. It
//       is not considered an Operator.
//     - A minus directly followed by a second minus is the start of a
//       Comment. It is not considered an Operator.
//     - A solidus (/) directly followed by an asterisk is the start of
//       a BlockComment. It is not considered an Operator.
//     - A QuotedIdentifier starts at a doublequote character ("), and
//       ends at the first following doublequote character which is
//       not escaped (doubled).
//     - A StringLiteral starts at a quote character ("), and ends at
//       the first following quote character which is not escaped
//       (doubled).
//     - A Comment starts from the double minus, and goes until the
//       first newline character (inclusive).
//     - A BlockComment starts at the '/*' combination, and ends at the
//       matching '*/' combination: these pairs may nest.
//
// The operators come from:
// https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#xref-delimiter%20token
// https://www.postgresql.org/docs/10/static/sql-syntax-lexical.html#SQL-SYNTAX-OPERATORS
//
// For the application I need there are too many token types, however
// implementation wise it would not be simpler to have less.

// The implementation is highly inspired by the standard library
// text/template/parse lexer, with the difference that it ranges of bytes
// instead of UTF8 runes. This simplification works because all of the
// SQL special characters are ASCII, and thanks to the nice property of
// UTF8 which guarantees that bytes representing valid ASCII characters
// must represent themselves.

import "strings"

type item struct {
	typ itemType // type of this item
	pos int      // starting position, in bytes, of this item
	val string   // value of this item
}

func (i item) String() string {
	switch i.typ {
	case itemEOF:
		return "<EOF>"
	case itemIdentifier:
		return "<Identifier>   " + string(i.val)
	case itemQuotedIdentifier:
		return `<"Identifier"> ` + string(i.val)
	case itemStringLiteral:
		return "<String>       " + string(i.val)
	case itemSpace:
		return "<Space>"
	case itemOperator:
		return "<Operator>     " + string(i.val)
	case itemNumeric:
		return "<Numeric>      " + string(i.val)
	case itemComment:
		return "<Comment>      " + string(i.val)
	case itemBlockComment:
		return "<BlockComment> " + string(i.val)
	default:
		return "<unknown>      " + string(i.val)
	}
}

type itemType int

const (
	itemEOF          itemType = iota
	itemSpace
	itemComment
	itemBlockComment
	itemIdentifier
	itemQuotedIdentifier
	itemStringLiteral
	itemNumeric
	itemOperator
)

type stateFn func(*lexer) stateFn

type lexer struct {
	input string    // input string to be tokenized
	pos   int       // current position in the input, in bytes
	start int       // start position of the current item, in bytes
	items chan item // channel of scanned items

	commentDepth int // current depth of nested block comment
}

var (
	spaceChars = " \t\r\n"
	operatorStart = "?()<=>|:.![],;+-^*/%"
	delimiters = spaceChars + "'\"()[],;$:+-*/<>=~!@#%^&|`?"
	oneCharOps = "()[],;.+-^*/%<>="
)

// lexSQL is the only function you should use outside this file.
// Example usage:
//
//     var query string
//     lex := lexSQL(query)
//     for token := range lex.items {
//             switch token.typ {
//             case itemIdentifier:
//                     println(token.val)
//             }
//     }
func lexSQL(input string) *lexer {
	l := &lexer{
		input: input,
		items: make(chan item),
	}
	go l.run()
	return l
}

const eof = -1

// next returns the next byte cast to a rune, using -1 for EOF.
// There is no need to actually the UTF8 byte sequences: we are only
// interested in valid ASCII characters.
func (l *lexer) next() rune {
	r := rune(0)
	if l.pos >= len(l.input) {
		r = eof
	} else {
		r = rune(l.input[l.pos])
	}
	l.pos++
	return r
}

func (l *lexer) backup() {
	l.pos--
}

func (l *lexer) peek() rune {
	if l.pos >= len(l.input) {
		return eof
	}
	return rune(l.input[l.pos])
}

func (l *lexer) emit(t itemType) {
	if l.pos > len(l.input) {
		l.pos = len(l.input)
	}
	l.items <- item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
}

func (l *lexer) run() {
	for state := lexAny; state != nil; {
		state = state(l)
	}
	close(l.items)
}

func lexAny(l *lexer) stateFn {
	c := l.next()
	switch c {
	case eof:
		l.emit(itemEOF)
		return nil
	case '\'':
		return lexSingleQuoted
	case '"':
		return lexDoubleQuoted
	case '-':
		if l.peek() == '-' {
			l.next()
			return lexComment
		}
	case '/':
		if l.peek() == '*' {
			l.next()
			return lexBlockComment
		}
	case '.':
		if n := l.peek(); n >= '0' && n <= '9' {
			return lexNumeric
		}
	}
	if c >= '0' && c <= '9' {
		return lexNumeric
	}
	if strings.IndexRune(spaceChars, c) >= 0 {
		return lexSpace
	}
	if strings.IndexRune(operatorStart, c) >= 0 {
		return lexOperator
	}
	return lexIdentifier
}

func lexSpace(l *lexer) stateFn {
	for strings.IndexRune(spaceChars, l.next()) >= 0 {
	}
	l.backup()
	l.emit(itemSpace)
	return lexAny
}

func lexOperator(l *lexer) stateFn {
	in := l.input[l.start:]
	if len(in) >= 3 && strings.HasPrefix(in, "??") {
		if in[2] == '(' || in[2] == ')' {
			l.pos += 2
			l.emit(itemOperator)
			return lexAny
		}
	}
	if len(in) >= 2 {
		switch string(in[:2]) {
		case "<=", "<>", ">=", "||", "::", "..", "->", "!=":
			l.pos += 1
			l.emit(itemOperator)
			return lexAny
		}
	}
	if strings.IndexByte(oneCharOps, in[0]) >= 0 {
		l.emit(itemOperator)
		return lexAny
	}
	return lexIdentifier
}

func lexIdentifier(l *lexer) stateFn {
	for {
		c := l.next()
		if c == eof || strings.IndexRune(delimiters, c) >= 0 {
			l.backup()
			l.emit(itemIdentifier)
			return lexAny
		}
	}
}

func lexQuoted(l *lexer, q byte) stateFn {
	for {
		i := strings.IndexByte(l.input[l.pos:], q)
		if i >= 0 {
			l.pos += i + 1
		} else {
			l.pos = len(l.input)
		}
		if l.peek() != rune(q) {
			break
		}
	}
	l.emit(itemStringLiteral)
	return lexAny
}

func lexSingleQuoted(l *lexer) stateFn {
	return lexQuoted(l, '\'')
}

func lexDoubleQuoted(l *lexer) stateFn {
	return lexQuoted(l, '"')
}

func lexNumeric(l *lexer) stateFn {
	for {
		c := l.next()
		switch {
		case c >= '0' && c <= '9':
		default:
			l.backup()
			l.emit(itemNumeric)
			return lexAny
		}
	}
}

// lexComment scans the current comment until the next newline. We know
// the comment has already started.
func lexComment(l *lexer) stateFn {
	if i := strings.IndexByte(l.input[l.pos:], '\n'); i >= 0 {
		l.pos += i + 1 // consume the newline too
	} else {
		l.pos = len(l.input)
	}
	l.emit(itemComment)
	return lexAny
}

func lexBlockComment(l *lexer) stateFn {
	depth := 1
	for {
		switch l.next() {
		case eof:
			l.emit(itemBlockComment)
			l.emit(itemEOF)
			return nil
		case '*':
			if l.peek() == '/' {
				l.next()
				depth--
			}
		case '/':
			if l.peek() == '*' {
				l.next()
				depth++
			}
		}
		if depth <= 0 {
			break
		}
	}
	l.emit(itemBlockComment)
	return lexAny
}
