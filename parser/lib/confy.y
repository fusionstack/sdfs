%{
#include <stdio.h>
#include "configure.h"

extern int yylex(void);
extern void yyerror(char *s);
extern int get_nfs_export(const char *path, const char *ip, const char *permision);
extern int get_networks(const char *_ip);
extern int set_value(const char* key, const char* value, int type);
%}

%defines

%union {
    double value;
    char *string;
}

%token MDS CDS YNFS YISCSI YWEB LOG C60 GLOBALS SEMICOLON OBRACE EBRACE
%token <string> IPADDRMASK
%token <string> PATH EXPORT RATE_LIMIT NETWORKS
%token <string> IPADDRESS
%token <string> WORD
%token <string> STATE
%token <string> NUMBER
%token <string> IQN
%token <string> MASTER_VIP
%token <string> ZK_HOSTS

%start conf

%%

conf:  subapps
    ;

 /*
 conf:  subapps global_values 
    | global_values
    ;

global_values: global_value
             | global_values global_value
             ;

global_value: local_value
            ;
 */

subapps: subapp
       | subapps subapp 
       ;

subapp: app app_subsets
      ;

app: YNFS
   | YISCSI
   | YWEB
   | MDS
   | CDS
   | LOG
   | C60
   | GLOBALS
   ;

app_subsets: OBRACE local_values sub_values EBRACE
           ;

local_values: 
            | local_values local_value
            ;

local_value: key_value SEMICOLON 
            ;

sub_values: 
          | sub_value sub_values
          ;

sub_value: export_value
         | ratelimit_value
         | networks_value
         ;

networks_value: NETWORKS OBRACE networks_lines EBRACE 
           ;

networks_lines: networks_line
            | networks_lines networks_line 
            ;

networks_line: IPADDRMASK SEMICOLON {
           get_networks($1);
           free($1);
           }
           ;

export_value: EXPORT OBRACE export_lines EBRACE 
      ;

export_lines: export_line
            | export_lines export_line 
            ;


export_line: PATH IPADDRESS '(' WORD ')' SEMICOLON {
           get_nfs_export($1, $2, $4);
           free($1);
           free($2);
           free($4);
           }
           ;

rate_lines: rate_line
          | rate_lines rate_line
          ;

ratelimit_value: RATE_LIMIT OBRACE rate_lines EBRACE
               ;

rate_line: PATH NUMBER SEMICOLON  {
           free($1);
           free($2);
         }
         ;

key_value: WORD NUMBER {
         set_value($1, $2, V_NUMBER);
         free($1);
         free($2);
         }
         ;

key_value: WORD WORD {
         set_value($1, $2, V_STRING);
         free($1);
         free($2);
         }
         ;

key_value: WORD STATE {
         set_value($1, $2, V_STATE);
         free($1);
         free($2);
         }
         ;

key_value: WORD PATH  {
         set_value($1, $2, V_STRING);
         free($1);
         free($2);
         }
         ;

key_value: WORD IPADDRESS {
         set_value($1, $2, V_STRING);
         free($1);
         free($2);
         }
         ;

key_value: WORD IPADDRMASK {
         set_value($1, $2, V_STRING);
         free($1);
         free($2);
         }
         ;

key_value: WORD IQN {
         set_value($1, $2, V_STRING);
         free($1);
         free($2);
         }
         ;

key_value: WORD MASTER_VIP {
         set_value($1, $2, V_STRING);
         free($1);
         free($2);
         }
         ;

key_value: WORD ZK_HOSTS{
         set_value($1, $2, V_STRING);
         free($1);
         free($2);
         }
         ;
%%
