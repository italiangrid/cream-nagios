#!/bin/bash
#****** WN/WN-softver
# NAME
# WN-softver - Detect the version of software which is really installed on the WN. 
# To detect the version lcg-version, glite-version commands and the cat of the 
# /etc/emi-version file are tried and if the commands are not available the script exits with an error.
#
# AUTHOR
#
# SAM Team same-devel[at]cern.ch
#
# LAST UPDATED (fixing GGUS ticket 90768)
#
# 2014-01-27
#
# LANGUAGE
#
# bash
#
# SOURCE

set -x
versionFilter="^2\.|^3\.|^1\.|^4\."
type="unknow"
mwver="error"
if [ -f /etc/umd-release ]; then
    type="UMD"
    mwver=`cat /etc/umd-release | awk '{print $3}'`
elif [ -f glite-version ] ; then
    type="gLite"
    mwver=`glite-version`
elif [ -f $EMI_TARBALL_BASE/etc/emi-version ] ; then
    type="EMI"
    mwver=`cat $EMI_TARBALL_BASE/etc/emi-version`
elif [ -f lcg-version ]; then
    type="LCG"
    mwver=`lcg-version`
else
    echo "ERROR: [glite|lcg|emi]-version was not found in `hostname -s`"
    exit 1
fi

set +x
#echo "Version pattern: $versionFilter"
#echo "$HOSTNAME: Deducted middleware version is $type $mwver"
#echo "summary: $type $mwver"
if ( echo $mwver | egrep -e $versionFilter > /dev/null 2>&1 ) ; then
    echo "`hostname -s` has $type $mwver"
else
    exit 1
fi
#****
