FROM alpine:3.6
MAINTAINER colin.hom@coreos.com

ADD ./bin/bgp-reflector-agent /bin/bgp-reflector-agent
ENTRYPOINT /bin/bgp-reflector-agent
