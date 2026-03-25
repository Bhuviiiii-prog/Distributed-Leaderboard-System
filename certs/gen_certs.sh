#!/bin/bash
# TODO: Keep cert generation script aligned with server TLS hostnames and IP SANs.
echo "[*] Generating SSL certificates..."

cat > san.cnf << 'EOF'
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no
[req_distinguished_name]
C = IN
ST = Karnataka
L = Bangalore
O = JackfruitProject
CN = localhost
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

openssl req -x509 -newkey rsa:4096 \
  -keyout server.key -out server.crt \
  -days 365 -nodes -config san.cnf

echo ""
echo "[+] Certificate details:"
openssl x509 -in server.crt -text -noout | grep -E "Subject:|Not Before:|Not After:|DNS:|IP"
echo ""
echo "[+] Done! Files: server.crt  server.key"
