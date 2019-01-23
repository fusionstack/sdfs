#!/bin/bash

for f in `find -name .svn -type d `;do rm -rf $f;done
