import gssapi
import os
import redis
import subprocess
import struct
# 下面KRB5CCNAME、KRB5_CONFIG、KRB5_KTNAME参数请根据实际情况修改
os.environ["KRB5_KTNAME"] = "/srv/project/krb5-example/user.keytab"
os.environ["KRB5_CONFIG"] = "/srv/project/krb5-example/krb5.conf"
os.environ["KRB5CCNAME"] = "/srv/project/krb5-example/krb5cc_0"
# 域名需要按照实际情况修改
realm = "HADOOP.COM"
# 用户名和域名需要按照实际情况修改
principal="redisuser@" + realm
store = {'ccache': os.environ["KRB5CCNAME"], 'keytab': os.environ["KRB5_KTNAME"]}
# 连接Redis实例业务IP及端口，可以根据实际情况修改
r = redis.Redis("192.168.20.238", 22400)
conn_pool = r.connection_pool
conn = conn_pool.get_connection("_")
subprocess.run(["kinit", "-kt", os.environ["KRB5_KTNAME"], principal])
server_principal = "redis/hadoop." + realm.lower() + "@" + realm
service_name = gssapi.Name(server_principal)
cname = service_name.canonicalize(gssapi.MechType.kerberos)
client_creds = gssapi.Credentials(usage='both', store=store)
client_ctx = gssapi.SecurityContext(name=cname,
                         flags=gssapi.RequirementFlag.mutual_authentication,
                         mech=gssapi.MechType.kerberos,
                         creds=client_creds)
SASL_QOP_AUTH = 1
server_token = None
while not client_ctx.complete:
    client_token = client_ctx.step(server_token)
    client_token = client_token or b''
    conn.send_command("authext", client_token)
    auth_response=conn.read_response()
    server_token=auth_response[4:]

msg = client_ctx.unwrap(server_token).message
qop = struct.pack('b', SASL_QOP_AUTH & msg[0])
msg = qop + msg[1:]
msg = client_ctx.wrap(msg + principal.encode(), False).message
conn.send_command("authext", msg)
auth_response=conn.read_response()
print(auth_response)

conn.send_command("config", "get", "maxmemory")
auth_response=conn.read_response()
print(auth_response)