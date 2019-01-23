%{
#include <stdio.h>
#include "confy.h"

extern int line;
%}

%option nounput
%option noinput
%option noyy_scan_string
%option noyy_scan_bytes
%option noyy_scan_buffer

%%

"globals"   { return GLOBALS; }

"mds"                 { return MDS; }

"cds"                 { return CDS; }

"yweb"                { return YWEB; }

"ynfs"                { return YNFS; }

"yiscsi"              { return YISCSI; }

"log"                 { return LOG; }

"c60"                 { return C60; }

"networks"         {
    yylval.string = strdup(yytext);
    return NETWORKS;
    }

"export"              { 
    yylval.string = strdup(yytext);
    return EXPORT; 
    }

"rate_limit"                { 
    yylval.string = strdup(yytext);
    return RATE_LIMIT; 
    }

"off" {
    yylval.string = strdup(yytext);
    return STATE;
}

"on" {
    yylval.string = strdup(yytext);
    return STATE;
}

[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\/[0-9]{1,2} {
    yylval.string = strdup(yytext);
    return IPADDRMASK;
}

\/[a-zA-Z0-9\/_]{0,512} { 
    yylval.string = strdup(yytext);
    return PATH;
}

[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3} {
    yylval.string = strdup(yytext);
    return IPADDRESS;
}

([a-zA-Z0-9_]{0,512}:[a-zA-Z0-9_]{0,512}\,){1,7}[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\/[0-9]{1,2} {
    yylval.string = strdup(yytext);
    return MASTER_VIP;
}

([a-zA-Z0-9_.-]{0,512}:[0-9]{1,6}\,)*[a-zA-Z0-9_.-]{0,512}:[0-9]{1,6} {
    yylval.string = strdup(yytext);
    return ZK_HOSTS;
}

[0-9]+[kmgKMG]?+[bB]? {
    yylval.string = strdup(yytext);
    return NUMBER;
}

[a-zA-Z0-9_]{0,512} {
    yylval.string = strdup(yytext);
    return WORD;
}

iqn\.[0-9]{4}\-[0-9]{2}\.com\.[a-zA-Z0-9_]+ {
        yylval.string = strdup(yytext);
        return IQN;
}

[()]                  { return *yytext; }

\{                    { return OBRACE; }

\}                    { return EBRACE; }

\n                    { line++; }

;                     { return SEMICOLON; }

[ \r\t]+               /* ignore whitespace */

\#[^\n]*               /* ignore comment */

.   {
    printf("nuknow char %c\n", *yytext);
    return 0;
}

%%


