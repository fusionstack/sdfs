#!/bin/sh

find . -name "*.[cch]" | xargs grep $1
