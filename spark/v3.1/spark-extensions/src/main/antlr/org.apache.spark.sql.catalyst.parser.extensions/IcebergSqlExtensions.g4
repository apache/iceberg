/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is an adaptation of Presto's and Spark's grammar files.
 */

grammar IcebergSqlExtensions;

@lexer::members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }

  /**
   * This method will be called when we see '/*' and try to match it as a bracketed comment.
   * If the next character is '+', it should be parsed as hint later, and we cannot match
   * it as a bracketed comment.
   *
   * Returns true if the next character is '+'.
   */
  public boolean isHint() {
    int nextChar = _input.LA(1);
    if (nextChar == '+') {
      return true;
    } else {
      return false;
    }
  }
}

singleStatement
    : statement EOF
    ;

statement
    : CALL multipartIdentifier '(' (callArgument (',' callArgument)*)? ')'                  #call
    | ALTER TABLE multipartIdentifier ADD PARTITION FIELD transform (AS name=identifier)?   #addPartitionField
    | ALTER TABLE multipartIdentifier DROP PARTITION FIELD transform                        #dropPartitionField
    | ALTER TABLE multipartIdentifier REPLACE PARTITION FIELD transform WITH transform (AS name=identifier)? #replacePartitionField
    | ALTER TABLE multipartIdentifier WRITE writeSpec                                       #setWriteDistributionAndOrdering
    | ALTER TABLE multipartIdentifier SET IDENTIFIER_KW FIELDS fieldList                    #setIdentifierFields
    | ALTER TABLE multipartIdentifier DROP IDENTIFIER_KW FIELDS fieldList                   #dropIdentifierFields
    | ALTER TABLE multipartIdentifier createReplaceBranchClause                             #createOrReplaceBranch
    | ALTER TABLE multipartIdentifier DROP BRANCH (IF EXISTS)? identifier                   #dropBranch
    ;

createReplaceBranchClause
    : (CREATE OR)? REPLACE BRANCH identifier branchOptions
    | CREATE BRANCH (IF NOT EXISTS)? identifier branchOptions
    ;

branchOptions
    : (AS OF VERSION snapshotId)? (refRetain)? (snapshotRetention)?
    ;

snapshotRetention
    : WITH SNAPSHOT RETENTION minSnapshotsToKeep
    | WITH SNAPSHOT RETENTION maxSnapshotAge
    | WITH SNAPSHOT RETENTION minSnapshotsToKeep maxSnapshotAge
    ;

refRetain
    : RETAIN number timeUnit
    ;

maxSnapshotAge
    : number timeUnit
    ;

minSnapshotsToKeep
    : number SNAPSHOTS
    ;

writeSpec
    : (writeDistributionSpec | writeOrderingSpec)*
    ;

writeDistributionSpec
    : DISTRIBUTED BY PARTITION
    ;

writeOrderingSpec
    : LOCALLY? ORDERED BY order
    | UNORDERED
    ;

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

order
    : fields+=orderField (',' fields+=orderField)*
    | '(' fields+=orderField (',' fields+=orderField)* ')'
    ;

orderField
    : transform direction=(ASC | DESC)? (NULLS nullOrder=(FIRST | LAST))?
    ;

transform
    : multipartIdentifier                                                       #identityTransform
    | transformName=identifier
      '(' arguments+=transformArgument (',' arguments+=transformArgument)* ')'  #applyTransform
    ;

transformArgument
    : multipartIdentifier
    | constant
    ;

expression
    : constant
    | stringMap
    | stringArray
    ;

constant
    : number                          #numericLiteral
    | booleanValue                    #booleanLiteral
    | STRING+                         #stringLiteral
    | identifier STRING               #typeConstructor
    ;

stringMap
    : MAP '(' constant (',' constant)* ')'
    ;

booleanValue
    : TRUE | FALSE
    ;

stringArray
    : ARRAY '(' constant (',' constant)* ')'
    ;

number
    : MINUS? EXPONENT_VALUE           #exponentLiteral
    | MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? FLOAT_LITERAL            #floatLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

fieldList
    : fields+=multipartIdentifier (',' fields+=multipartIdentifier)*
    ;

nonReserved
    : ADD | ALTER | AS | ASC | BRANCH | BY | CALL | CREATE | DAYS | DESC | DROP | EXISTS | FIELD | FIRST | HOURS | IF | LAST | NOT | NULLS | OF | OR | ORDERED | PARTITION | TABLE | WRITE
    | DISTRIBUTED | LOCALLY | MINUTES | MONTHS | UNORDERED | REPLACE | RETAIN | RETENTION | VERSION | WITH | IDENTIFIER_KW | FIELDS | SET | SNAPSHOT | SNAPSHOTS
    | TRUE | FALSE
    | MAP
    ;

snapshotId
    : number
    ;

timeUnit
    : DAYS
    | HOURS
    | MINUTES
    ;

ADD: 'ADD';
ALTER: 'ALTER';
AS: 'AS';
ASC: 'ASC';
BRANCH: 'BRANCH';
BY: 'BY';
CALL: 'CALL';
CREATE: 'CREATE';
DAYS: 'DAYS';
DESC: 'DESC';
DISTRIBUTED: 'DISTRIBUTED';
DROP: 'DROP';
EXISTS: 'EXISTS';
FIELD: 'FIELD';
FIELDS: 'FIELDS';
FIRST: 'FIRST';
HOURS: 'HOURS';
IF : 'IF';
LAST: 'LAST';
LOCALLY: 'LOCALLY';
MINUTES: 'MINUTES';
MONTHS: 'MONTHS';
NOT: 'NOT';
NULLS: 'NULLS';
OF: 'OF';
OR: 'OR';
ORDERED: 'ORDERED';
PARTITION: 'PARTITION';
REPLACE: 'REPLACE';
RETAIN: 'RETAIN';
RETENTION: 'RETENTION';
IDENTIFIER_KW: 'IDENTIFIER';
SET: 'SET';
SNAPSHOT: 'SNAPSHOT';
SNAPSHOTS: 'SNAPSHOTS';
TABLE: 'TABLE';
UNORDERED: 'UNORDERED';
VERSION: 'VERSION';
WITH: 'WITH';
WRITE: 'WRITE';

TRUE: 'TRUE';
FALSE: 'FALSE';

MAP: 'MAP';
ARRAY: 'ARRAY';

PLUS: '+';
MINUS: '-';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

EXPONENT_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT {isValidDecimal()}?
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS {isValidDecimal()}?
    ;

FLOAT_LITERAL
    : DIGIT+ EXPONENT? 'F'
    | DECIMAL_DIGITS EXPONENT? 'F' {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
