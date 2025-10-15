###Use the following for certificate generation:###
1. openssl req -x509 -newkey rsa:2048 -keyout my_private_key_last.pem -out my_cert_last.pem -days 655 -nodes -addext "subjectAltName = URI:urn:example.org:FreeOpcUa:python-opcua"
and
openssl x509 -outform der -in my_cert_last.pem -out my_cert_last.der

2. move my_cert_last.der to /trusted

3. asyncua==1.1.6 
