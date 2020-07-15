/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
/**
 * A Protocol Buffers 3 grammar for ANTLR v4.
 *
 * Derived and adapted from:
 * https://developers.google.com/protocol-buffers/docs/reference/proto3-spec
 *
 * @author Marco Willemart
 */
grammar Protobuf3;

//
// Proto file
//

proto
    :   syntax (   importStatement
               |   packageStatement
               |   option
               |   topLevelDef
               |   emptyStatement
               )*
        EOF
    ;

//
// Syntax
//

syntax
    :   'syntax' '=' ('"proto3"' | '\'proto3\'' ) ';'
    ;

//
// Import Statement
//

importStatement
    :   'import' ('weak' | 'public')? StrLit ';'
    ;

//
// Package
//

packageStatement
    :   'package' fullIdent ';'
    ;

//
// Option
//

option
    :   'option' optionName '=' (constant | optionBody)  ';'
    ;

optionName
    :   (Ident | '(' fullIdent ')' ) ('.' (Ident | reservedWord))*
    ;

optionBody
    : '{'
        (optionBodyVariable)*
      '}'
    ;

optionBodyVariable
    : optionName ':' constant
    ;

//
// Top Level definitions
//

topLevelDef
   :   message
   |   enumDefinition
   |   extend
   |   service
   ;

// Message definition

message
    :   'message' messageName messageBody
    ;

messageBody
    :   '{' (   field
            |   enumDefinition
            |   message
            |   extend
            |   option
            |   oneof
            |   mapField
            |   reserved
            |   emptyStatement
            )*
       '}'
    ;

// Enum definition

enumDefinition
    :   'enum' enumName enumBody
    ;

enumBody
    :   '{' (   option
            |   enumField
            |   emptyStatement
            )*
        '}'
    ;

enumField
    :   Ident '=' '-'? IntLit ('[' enumValueOption (','  enumValueOption)* ']')? ';'
    ;

enumValueOption
    :   optionName '=' constant
    ;

// Extend definition
//
// NB: not defined in the spec but supported by protoc and covered by protobuf3 tests
//     see e.g. php/tests/proto/test_import_descriptor_proto.proto
//     of https://github.com/protocolbuffers/protobuf
//

extend
    :   'extend' messageType '{' ( field
                                 | emptyStatement
                                 ) '}'
    ;

// Service definition

service
    :   'service' serviceName '{' (   option
                                  | COMMENT
                                  | LINE_COMMENT
                                  |   rpc
                                  // not defined in the protobuf specification
                                  //|   stream
                                  |   emptyStatement
                                  )*
        '}'
    ;

rpc
    :   'rpc' rpcName '(' 'stream'? messageType ')'
        'returns' '(' 'stream'? messageType ')' (('{' (option | emptyStatement)* '}') | ';')
    ;

//
// Reserved
//

reserved
    :   'reserved' (ranges | fieldNames) ';'
    ;

ranges
    :   rangeRule (',' rangeRule)*
    ;

    rangeRule
    :   IntLit
    |   IntLit 'to' IntLit
    ;

fieldNames
    :   StrLit (',' StrLit)*
    ;

//
// Fields
//

typeRule
    :   (   'double'
        |   'float'
        |   'int32'
        |   'int64'
        |   'uint32'
        |   'uint64'
        |   'sint32'
        |   'sint64'
        |   'fixed32'
        |   'fixed64'
        |   'sfixed32'
        |   'sfixed64'
        |   'bool'
        |   'string'
        |   'bytes'
        )
    |   messageOrEnumType
    ;

fieldNumber
    : IntLit
    ;

// Normal field

field
    :   'repeated'? typeRule fieldName '=' fieldNumber ('[' fieldOptions ']')? ';'
    ;

fieldOptions
    :   fieldOption (','  fieldOption)*
    ;

fieldOption
    :   optionName '=' constant
    ;

// Oneof and oneof field

oneof
    :   'oneof' oneofName '{' (oneofField | emptyStatement)* '}'
    ;

oneofField
    :   typeRule fieldName '=' fieldNumber ('[' fieldOptions ']')? ';'
    ;

// Map field

mapField
    :   'map' '<' keyType ',' typeRule '>' mapName '=' fieldNumber ('[' fieldOptions ']')? ';'
    ;

keyType
    :   'int32'
    |   'int64'
    |   'uint32'
    |   'uint64'
    |   'sint32'
    |   'sint64'
    |   'fixed32'
    |   'fixed64'
    |   'sfixed32'
    |   'sfixed64'
    |   'bool'
    |   'string'
    ;

reservedWord
    :   EXTEND
    |   MESSAGE
    |   OPTION
    |   PACKAGE
    |   RPC
    |   SERVICE
    |   STREAM
    |   STRING
    |   SYNTAX
    |   WEAK
    ;
//
// Lexical elements
//

// Keywords

BOOL            : 'bool';
BYTES           : 'bytes';
DOUBLE          : 'double';
ENUM            : 'enum';
EXTEND          : 'extend';
FIXED32         : 'fixed32';
FIXED64         : 'fixed64';
FLOAT           : 'float';
IMPORT          : 'import';
INT32           : 'int32';
INT64           : 'int64';
MAP             : 'map';
MESSAGE         : 'message';
ONEOF           : 'oneof';
OPTION          : 'option';
PACKAGE         : 'package';
PROTO3_DOUBLE   : '"proto3"';
PROTO3_SINGLE   : '\'proto3\'';
PUBLIC          : 'public';
REPEATED        : 'repeated';
RESERVED        : 'reserved';
RETURNS         : 'returns';
RPC             : 'rpc';
SERVICE         : 'service';
SFIXED32        : 'sfixed32';
SFIXED64        : 'sfixed64';
SINT32          : 'sint32';
SINT64          : 'sint64';
STREAM          : 'stream';
STRING          : 'string';
SYNTAX          : 'syntax';
TO              : 'to';
UINT32          : 'uint32';
UINT64          : 'uint64';
WEAK            : 'weak';

// Letters and digits

fragment
Letter
    :   [A-Za-z_]
    ;

fragment
DecimalDigit
    :   [0-9]
    ;

fragment
OctalDigit
    :   [0-7]
    ;

fragment
HexDigit
    :   [0-9A-Fa-f]
    ;

// Identifiers

Ident
    :   Letter (Letter | DecimalDigit)*
    ;

fullIdent
    :   Ident ('.' Ident)*
    ;

messageName
    :   Ident
    ;

enumName
    :   Ident
    ;

messageOrEnumName
    :   Ident
    ;

fieldName
    :   Ident
    |   reservedWord
    ;

oneofName
    :   Ident
    ;

mapName
    :   Ident
    ;

serviceName
    :   Ident
    ;

rpcName
    :   Ident
    ;

messageType
    :   '.'? (Ident '.')* messageName
    ;

messageOrEnumType
    :   '.'? ( (Ident | reservedWord) '.')* messageOrEnumName
    ;

// Integer literals

IntLit
    :   DecimalLit
    |   OctalLit
    |   HexLit
    ;

fragment
DecimalLit
    :   [1-9] DecimalDigit*
    ;

fragment
OctalLit
    :   '0' OctalDigit*
    ;

fragment
HexLit
    :   '0' ('x' | 'X') HexDigit+
    ;

// Floating-point literals

FloatLit
    :   (   Decimals '.' Decimals? Exponent?
        |   Decimals Exponent
        |   '.' Decimals Exponent?
        )
    |   'inf'
    |   'nan'
    ;

fragment
Decimals
    :   DecimalDigit+
    ;

fragment
Exponent
    :   ('e' | 'E') ('+' | '-')? Decimals
    ;

// Boolean

BoolLit
    :   'true'
    |   'false'
    ;

// String literals

StrLit
    :   '\'' CharValue* '\''
    |   '"' CharValue* '"'
    ;

fragment
CharValue
    :   HexEscape
    |   OctEscape
    |   CharEscape
    |   ~[\u0000\n\\]
    ;

fragment
HexEscape
    :   '\\' ('x' | 'X') HexDigit HexDigit
    ;

fragment
OctEscape
    :   '\\' OctalDigit OctalDigit OctalDigit
    ;

fragment
CharEscape
    :   '\\' [abfnrtv\\'"]
    ;

Quote
    :   '\''
    |   '"'
    ;

// Empty Statement

emptyStatement
    :   ';'
    ;

// Constant

constant
    :   fullIdent
    |   ('-' | '+')? IntLit
    |   ('-' | '+')? FloatLit
    |   (   StrLit
        |   BoolLit
        )
    ;

// Separators

LPAREN          : '(';
RPAREN          : ')';
LBRACE          : '{';
RBRACE          : '}';
LBRACK          : '[';
RBRACK          : ']';
LCHEVR          : '<';
RCHEVR          : '>';
SEMI            : ';';
COMMA           : ',';
DOT             : '.';
MINUS           : '-';
PLUS            : '+';

// Operators

ASSIGN          : '=';

// Whitespace and comments

WS  :   [ \t\r\n\u000C]+ -> skip
    ;

COMMENT
    :   '/*' .*? '*/'
    ;

LINE_COMMENT
    :   '//' ~[\r\n]*
    ;
