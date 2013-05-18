#!/bin/bash
#
# Generate an SSL certificate for use by WMRServer with user input
#
# This script is normally interactive, prompting the user for necessary input
# such as passwords. However, it can be run without requiring any interaction
# if the following environment variables are set:
#
#  KEYSTORE_FILE - The path to the keystore file to create
#  KEYSTORE_PASS - The password to use for the keystore and private key
#  CNAME         - The common name (hostname) to use in the server certificate
# 
# With only KEYSTORE_PASS set, newlines can be supplied repeatedly to accept
# the default values for the other two. Be warned, however, that the supplying
# passwords in this way is inherently insecure.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/wmr-config.sh

# Fail on errors
set -e


CA_DIR=$WMR_HOME/conf/CA

DEFAULT_CNAME=`hostname`
DEFAULT_KEYSTORE_FILE=$WMR_HOME/conf/keystore.jks


# Print instructions
CA_DIR_ECHO=${CA_DIR/#${WMR_HOME}/\$WMR_HOME}
cat - <<EOF
==========================================================================

This script will generate an SSL certificate signed by the CA in:
  $CA_DIR_ECHO
(generating the CA certificate if necessary) and store it in a keystore
for use by WMRServer.

==========================================================================


EOF


# Prompt for keystore file
if [ -z "$KEYSTORE_FILE" ]; then
	cat - <<EOF
Choose the location of your keystore file, or press enter for the default.

IMPORTANT: Keep this file secure! Ideally, its entire parent directory
should only be readable by WMRServer and any administrators who are
responsible for it. If the directory you choose is insecure, quit this
script (Ctrl+C) and retry once it has the correct permissions.

EOF
	DEFAULT_KEYSTORE_FILE_ECHO=${DEFAULT_KEYSTORE_FILE/#${WMR_HOME}/\$WMR_HOME}
	read -p "> Enter the keystore location [$DEFAULT_KEYSTORE_FILE_ECHO]: " \
			KEYSTORE_FILE
	if [ -z "$KEYSTORE_FILE" ]; then
		KEYSTORE_FILE=$DEFAULT_KEYSTORE_FILE
	fi
	echo
	echo
fi

# Check whether keystore exists
if [ -e $KEYSTORE_FILE ]; then
	echo "WARNING: Keystore at $KEYSTORE_FILE already exists!"
	read -p "Continue (y|N)? "
	if [ "$REPLY" != "y" ]; then
		exit 1
	fi
	echo
	echo
fi


# Prompt for cname
if [ -z "$CNAME" ]; then
	cat - <<EOF
Choose the hostname that should appear in the server certificate, or press
enter for the default.

EOF
	read -p "> Enter the hostname [$DEFAULT_CNAME]: " CNAME
	if [ -z "$CNAME" ]; then
		CNAME=$DEFAULT_CNAME
	fi
	echo
	echo
fi


# Prompt for password
if [ -z "$KEYSTORE_PASS" ]; then
	cat - <<EOF
Choose the password which will be used to protect the integrity of the keystore
AND to keep the private key secret. It must be at least 6 characters long. Be
prepared to enter it in the \$WMR_HOME/conf/wmr-site.xml file once this script
is finished.

EOF

	while [ -z "$KEYSTORE_PASS" ]; do
		read -sp "> Enter new keystore password: " KEYSTORE_PASS
		echo
		
		# Check length
		if [ ${#KEYSTORE_PASS} -lt 6 ]; then
			echo "ERROR: Password must be at least 6 characters. Please try again."
			KEYSTORE_PASS=
			continue
		fi
		
		# Confirm
		read -sp "> Reenter new keystore password: " KEYSTORE_PASS_CONFIRM
		echo
		if [ "$KEYSTORE_PASS_CONFIRM" != "$KEYSTORE_PASS" ]; then
			echo "ERROR: Passwords do not match. Please try again."
			KEYSTORE_PASS=
		fi
		KEYSTORE_PASS_CONFIRM=
	done
	
	echo
	echo
fi


# Generate CA certificate if not present
if [ ! -e $CA_DIR/ca-cert.pem ]; then
	echo ">>> Generating CA certificate..."
	make -C $CA_DIR ca-cert.pem cn="WebMapReduce CA at $(hostname)"
	echo
fi

# Import CA as a trusted certificate
echo ">>> Importing CA certificate into keystore..."
keytool -importcert -noprompt -alias "wmr-ca" -keystore $KEYSTORE_FILE \
    -file $CA_DIR/ca-cert.pem -storepass $KEYSTORE_PASS
echo


# Generate server key and certificate signing request
echo ">>> Generating server private key and signing request..."
keytool -genkeypair -alias jetty -keystore $KEYSTORE_FILE -keyalg RSA \
    -dname "CN=$CNAME" -storepass $KEYSTORE_PASS -keypass $KEYSTORE_PASS
keytool -certreq -alias jetty -keystore $KEYSTORE_FILE \
    -file $CA_DIR/wmr-server.csr -storepass $KEYSTORE_PASS 
echo

# Sign CSR with CA
echo ">>> Signing server certificate..."
make -C $CA_DIR sign
echo

# Import resulting server certificate
echo ">>> Importing server certificate into keystore..."
keytool -importcert -alias jetty -keystore $KEYSTORE_FILE \
    -file $CA_DIR/wmr-server.crt -storepass $KEYSTORE_PASS
echo


# Give final instructions
KEYSTORE_FILE_ECHO=${KEYSTORE_FILE/#${WMR_HOME}/\$WMR_HOME}
KEYSTORE_FILE_PROP=${KEYSTORE_FILE/#${WMR_HOME}/\$\{wmr.home.dir\}}
cat - <<EOF
==========================================================================

Server SSL certificate successfully generated and stored in the keystore:
  $KEYSTORE_FILE_ECHO

Now add the following lines to \$WMR_HOME/conf/wmr-site.xml (or uncomment
them if they are already present), replacing "YOUR-PASSWORD" with the
password you entered above:

  <property>
    <name>wmr.server.ssl.enable</name>
    <value>true</value>
  </property>

  <property>
    <name>wmr.server.ssl.keystore</name>
    <value>$KEYSTORE_FILE_PROP</value>
  </property>

  <property>
    <name>wmr.server.ssl.keystore.password</name>
    <value>YOUR-PASSWORD</value>
  </property>

IMPORTANT: Again, be sure the keystore file is secure! Since you are
storing a password in your wmr-site.xml file, this should be secure too.
A good guideline is to keep the entire \$WMR_HOME/conf directory readable
only by WMRServer and any administrators who are responsible for it.

Copy this CA certificate to your frontend and use its path as the value of
the setting \$wmrSSLCAInfo in include/settings.php:
  $CA_DIR_ECHO/ca-cert.pem 

==========================================================================
EOF
