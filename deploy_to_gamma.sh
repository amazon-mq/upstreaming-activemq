# replace the following with your primar/repica ips and bastions in their accounts:
primary_host=ec2-18-233-7-109.compute-1.amazonaws.com
primary_bastion=ec2-34-224-113-166.compute-1.amazonaws.com
replica_host=18.117.12.135
replica_bastion=ec2-18-220-220-164.us-east-2.compute.amazonaws.com

if [[ $1 == "clear" ]]
then
  ssh -oProxyJump=${primary_bastion} ${primary_host} "sudo /apollo/env/AmazonMQActiveMQ/bin/env-deactivate && sudo rm -fr /data/kahadb /data/activemq_default_data_dir /apollo/env/AmazonMQActiveMQ/var/output/logs/activemq.log"
  ssh -oProxyJump=${replica_bastion} ${replica_host} "sudo /apollo/env/AmazonMQActiveMQ/bin/env-deactivate && sudo rm -fr /data/kahadb /data/activemq_default_data_dir /apollo/env/AmazonMQActiveMQ/var/output/logs/activemq.log"
  ssh -oProxyJump=${primary_bastion} ${primary_host} "sudo /apollo/env/AmazonMQActiveMQ/bin/env-activate"
  ssh -oProxyJump=${replica_bastion} ${replica_host} "sudo /apollo/env/AmazonMQActiveMQ/bin/env-activate"
fi

function copy() {
  host=$1
  bastion=$2
  scp -oProxyJump=${bastion} assembly/target/apache-activemq-5.18.0-SNAPSHOT-bin.tar.gz ${host}:/tmp
  ssh -oProxyJump=${bastion} ${host} "cd /apollo/env/AmazonMQActiveMQ && sudo cp /tmp/apache-activemq-5.18.0-SNAPSHOT-bin.tar.gz . && sudo mv activemq-root-env activemq-root-env.bak_$(date +%s) && sudo tar -xzf apache-activemq-5.18.0-SNAPSHOT-bin.tar.gz && sudo mv apache-activemq-5.18.0-SNAPSHOT activemq-root-env"
  ssh -oProxyJump=${bastion} ${host} "ps xua | grep activemq.jar|grep -v grep | awk '{print \$2}' | head -1 | xargs sudo kill"
}

copy ${primary_host} ${primary_bastion}
copy ${replica_host} ${replica_bastion}

echo "use the following command to check replica logs:"
echo "ssh -oProxyJump=${replica_bastion} ${replica_host} \"tail -f /apollo/env/AmazonMQActiveMQ/var/output/logs/activemq.log\""