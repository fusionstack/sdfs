#!/usr/bin/env python
# -*- coding: utf-8 -*-

havecolor=1

esc_seq = "\x1b["

g_attr = {}
g_attr["normal"]       =  0

g_attr["bold"]         =  1
g_attr["faint"]        =  2
g_attr["standout"]     =  3
g_attr["underline"]    =  4
g_attr["blink"]        =  5
g_attr["overline"]     =  6  # Why is overline actually useful?
g_attr["reverse"]      =  7
g_attr["invisible"]    =  8

g_attr["no-attr"]      = 22
g_attr["no-standout"]  = 23
g_attr["no-underline"] = 24
g_attr["no-blink"]     = 25
g_attr["no-overline"]  = 26
g_attr["no-reverse"]   = 27
g_attr["black"]        = 30
g_attr["red"]          = 31
g_attr["green"]        = 32
g_attr["yellow"]       = 33
g_attr["blue"]         = 34
g_attr["magenta"]      = 35
g_attr["cyan"]         = 36
g_attr["white"]        = 37
g_attr["default"]      = 39
g_attr["bg_black"]     = 40
g_attr["bg_red"]       = 41
g_attr["bg_green"]     = 42
g_attr["bg_yellow"]    = 43
g_attr["bg_blue"]      = 44
g_attr["bg_magenta"]   = 45
g_attr["bg_cyan"]      = 46
g_attr["bg_white"]     = 47
g_attr["bg_default"]   = 49

def color(fg, bg="default", attr=["normal"]):
    mystr = esc_seq[:] + "%02d" % g_attr[fg]
    for x in [bg]+attr:
        mystr += ";%02d" % g_attr[x]
    return mystr+"m"

codes={}
codes["reset"]     = esc_seq + "39;49;00m"

codes["bold"]      = esc_seq + "01m"
codes["faint"]     = esc_seq + "02m"
codes["standout"]  = esc_seq + "03m"
codes["underline"] = esc_seq + "04m"
codes["blink"]     = esc_seq + "05m"
codes["overline"]  = esc_seq + "06m"  
codes["reverse"]   = esc_seq + "07m"

ansi_color_codes = []
for x in xrange(30, 38):
    ansi_color_codes.append("%im" % x)
    ansi_color_codes.append("%i;01m" % x)

rgb_ansi_colors = ['0x000000', '0x555555', '0xAA0000', '0xFF5555', '0x00AA00',
    '0x55FF55', '0xAA5500', '0xFFFF55', '0x0000AA', '0x5555FF', '0xAA00AA',
    '0xFF55FF', '0x00AAAA', '0x55FFFF', '0xAAAAAA', '0xFFFFFF']

for x in xrange(len(rgb_ansi_colors)):
    codes[rgb_ansi_colors[x]] = esc_seq + ansi_color_codes[x]

del x

codes["black"]     = codes["0x000000"]
codes["darkgray"]  = codes["0x555555"]

codes["red"]       = codes["0xFF5555"]
codes["darkred"]   = codes["0xAA0000"]

codes["green"]     = codes["0x55FF55"]
codes["darkgreen"] = codes["0x00AA00"]

codes["yellow"]    = codes["0xFFFF55"]
codes["brown"]     = codes["0xAA5500"]

codes["blue"]      = codes["0x5555FF"]
codes["darkblue"]  = codes["0x0000AA"]

codes["fuchsia"]   = codes["0xFF55FF"]
codes["purple"]    = codes["0xAA00AA"]

codes["turquoise"] = codes["0x55FFFF"]
codes["teal"]      = codes["0x00AAAA"]

codes["white"]     = codes["0xFFFFFF"]
codes["lightgray"] = codes["0xAAAAAA"]

codes["darkteal"]   = codes["turquoise"]

codes["0xAAAA00"]   = codes["brown"]
codes["darkyellow"] = codes["0xAAAA00"]

codes["bg_black"]      = esc_seq + "40m"
codes["bg_darkred"]    = esc_seq + "41m"
codes["bg_darkgreen"]  = esc_seq + "42m"
codes["bg_brown"]      = esc_seq + "43m"
codes["bg_darkblue"]   = esc_seq + "44m"
codes["bg_purple"]     = esc_seq + "45m"
codes["bg_teal"]       = esc_seq + "46m"
codes["bg_lightgray"]  = esc_seq + "47m"

codes["bg_darkyellow"] = codes["bg_brown"]

# Colors from /etc/init.d/functions.sh
codes["NORMAL"]     = esc_seq + "0m"
codes["GOOD"]       = codes["green"]
codes["WARN"]       = codes["yellow"]
codes["BAD"]        = codes["red"]
codes["HILITE"]     = codes["teal"]
codes["BRACKET"]    = codes["blue"]

# Portage functions
codes["INFORM"]                  = codes["darkgreen"]

#codes["UNMERGE_WARN"]            = codes["red"]
codes["DELETE_WARN"]            = codes["red"]

codes["SECURITY_WARN"]           = codes["red"]
codes["MERGE_LIST_PROGRESS"]     = codes["yellow"]
codes["PKG_BLOCKER"]             = codes["red"]
codes["PKG_BLOCKER_SATISFIED"]   = codes["darkblue"]
codes["PKG_MERGE"]               = codes["darkgreen"]
codes["PKG_MERGE_SYSTEM"]        = codes["darkgreen"]
codes["PKG_MERGE_WORLD"]         = codes["green"]
codes["PKG_UNINSTALL"]           = codes["red"]
codes["PKG_NOMERGE"]             = codes["darkblue"]
codes["PKG_NOMERGE_SYSTEM"]      = codes["darkblue"]
codes["PKG_NOMERGE_WORLD"]       = codes["blue"]
codes["PROMPT_CHOICE_DEFAULT"]   = codes["green"]
codes["PROMPT_CHOICE_OTHER"]     = codes["red"]


def colorize(color_key, text):
    global havecolor
    if havecolor:
        return codes[color_key] + text + codes["reset"]
    else:
        return text

compat_functions_colors = ["bold","white","teal","turquoise","darkteal",
    "fuchsia","purple","blue","darkblue","green","darkgreen","yellow",
    "brown","darkyellow","red","darkred"]

def create_color_func(color_key):
    def derived_func(*args):
        newargs = list(args)
        newargs.insert(0, color_key)
        return colorize(*newargs)
    return derived_func

for c in compat_functions_colors:
    globals()[c] = create_color_func(c)

