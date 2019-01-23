#include "var.h"
#include <stdio.h>
#include <stdarg.h>

#define DBG_SUBSYS S_LIBYLIB

#include "dbg.h"

int varsum( int first, ... ) 
{
        int sum = 0;
        int i = first; 
        va_list marker; 

        va_start( marker, (first) );   

        while( i != -1 ) {
                sum += i; 
                i = va_arg(marker, int);
        }

        va_end( marker );     

        return sum;   
} 

