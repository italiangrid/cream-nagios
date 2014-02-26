#!/bin/bash

cd $(dirname $0)

#echo "Checking if CSH works"
rm -f env-csh.txt

# FZK and some others had problems passing the test with a separate csh script:
# cat <<EOF > csh-test.csh
# #!/bin/csh
# env | sort > env-csh.txt
# EOF

set -x
/bin/csh -c "env|sort" > env-csh.txt
set +x

#if [ "x$1" == "xdebug" ] ; then
#   echo "CSH script generated the following output:"
#   cat env-csh.txt
#fi

if ( grep PATH env-csh.txt > /dev/null ) ; then
    echo "`hostname -s` has csh"
    rm -f env-csh.txt
else
    echo "Test failed on `hostname -s`"
    cat env-csh.txt
    rm -f env-csh.txt
    exit 1
fi

