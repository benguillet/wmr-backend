# $Id: Makefile,v 1.4 2006/06/20 18:55:37 jmates Exp $
#
# NOTE If running less than OpenSSL 0.9.8a, see -newkey, below.
#
# Automates the setup of a custom Certificate Authority and provides
# routines for signing and revocation of certificates. To use, first
# customize the commands in this file and the settings in openssl.cnf,
# then run:
#
# make init
#
# Then, copy in certificate signing requests, and ensure their suffix is
# .csr before signing them with the following command:
#
# make sign
#
# To revoke a key, name the certificate file with the cert option
# as shown below:
#
# make revoke cert=foo.crt
#
# This will revoke the certificate and call gencrl; the revocation list
# will then need to be copied somehow to the various systems that use
# your CA cert.

requests = *.csr
cn = $(shell hostname)

sign: init ${requests}

# remove -batch option if want chance to not certify a particular request
${requests}: init FORCE
	@openssl ca -batch -config openssl.cnf -policy policy_anything -in $@ | \
	    awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/' > ${@:.csr=.crt}
	@[ -f ${@:.csr=.crt} ] && rm $@

revoke: init
	@test $${cert:?"usage: make revoke cert=certificate"}
	@openssl ca -config openssl.cnf -revoke $(cert)
	@$(MAKE) gencrl

gencrl: init
	@openssl ca -config openssl.cnf -gencrl -out ca-crl.pem

clean:
	-rm ${requests}

# creates required supporting files, CA key and certificate
init: crl newcerts private serial index ca-cert.pem

help:
	@echo make sign
	@echo '  - signs all *.csr files in this directory'
	@echo
	@echo make revoke cert=filename
	@echo '  - revokes certificate in named file and calls gencrl'
	@echo
	@echo make gencrl
	@echo '  - updates Certificate Revocation List (CRL)'
	@echo
	@echo make clean
	@echo '  - removes all *.csr files in this directory'
	@echo
	@echo make init [cn=common_name]
	@echo '  - required initial setup command for new CA'


# Init targets

crl:
	@mkdir crl
	
newcerts:
	@mkdir newcerts

private:
	@mkdir private
	@chmod go-rwx private
	  
serial:
	@echo '01' > serial

index:
	@touch index

ca-cert.pem:
	@$(MAKE) private # Don't use as a prerequisite--will always be out of date
	# NOTE use "-newkey rsa" if running less than OpenSSL 0.9.8a
	@echo "Using common name ${cn}"
	@openssl req -batch -new -nodes -config openssl.cnf -newkey rsa:2048 \
	  -x509 -extensions v3_ca -days 3650 \
	  -subj "/CN=${cn}" \
	  -keyout private/ca-key.pem \
	  -out ca-cert.pem

# for legacy make support
FORCE:
