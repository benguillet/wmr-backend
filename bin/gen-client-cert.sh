#!/bin/bash
#
# Generate an SSL certificate and private key for use in WebMapReduce interface
# servers if client certificate authentication is required.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/wmr-config.sh

# Fail on errors
set -e


CA_DIR=$WMR_HOME/conf/CA
CSR_FILE=$CA_DIR/wmr-client.csr
CERT_FILE=$CA_DIR/wmr-client.crt
KEY_FILE=$CA_DIR/private/wmr-client.key

DEFAULT_CNAME="localhost"


# Print instructions
CA_DIR_ECHO=${CA_DIR/#${WMR_HOME}/\$WMR_HOME}
cat - <<EOF
==========================================================================

This script will generate an SSL certificate signed by the CA in:
  $CA_DIR_ECHO
(generating the CA certificate if necessary) for use in WebMapReduce
interface servers if client authentication is required.

==========================================================================

EOF
echo

# Prompt for cname
if [ -z "$CNAME" ]; then
	cat - <<EOF
Choose the "common name" that should appear in the client certificate, or press
enter for the default. Unlike the hostname in the server certificate, this name
can be practically anything.

EOF
	read -p "Enter the common name [$DEFAULT_CNAME]: " CNAME
	if [ -z "$CNAME" ]; then
		CNAME=$DEFAULT_CNAME
	fi
	echo
fi


# Generate CA certificate if not present
if [ ! -e $CA_DIR/ca-cert.pem ]; then
	echo ">>> Generating CA certificate..."
	make -C $CA_DIR ca-cert.pem cn="WebMapReduce CA at $(hostname)"
	echo
fi

# Generate client private key and CSR
echo ">>> Generating client private key and signing request..."
openssl req -batch -new -nodes -newkey rsa:1024 \
    -subj "/CN=$CNAME" \
    -keyout $KEY_FILE \
    -out $CSR_FILE
echo

# Sign client CSR
echo ">>> Signing client certificate..."
make -C $CA_DIR sign
echo


# Give final instructions
CERT_FILE_ECHO=${CERT_FILE/#${WMR_HOME}/\$WMR_HOME}
KEY_FILE_ECHO=${KEY_FILE/#${WMR_HOME}/\$WMR_HOME}
KEYSTORE_FILE_PROP=${KEYSTORE_FILE/#${WMR_HOME}/\$\{wmr.home.dir\}}
cat - <<EOF
==========================================================================

Client SSL certificate successfully generated and stored in the file:
  $CERT_FILE_ECHO
The private key was stored in the file:
  $KEY_FILE_ECHO
Copy these files to your frontend and use their paths as the value of the
settings \$wmrSSLCert and \$wmrSSLKey, respectively.

If you have not done so already, add the following lines to the file
\$WMR_HOME/conf/wmr-site.xml (or uncomment them if they are already
present) to enable client certificate authentication:

  <property>
    <name>wmr.server.ssl.needclient</name>
    <value>true</value>
  </property>

==========================================================================
EOF
