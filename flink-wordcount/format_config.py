FLINK_CONF_TEMPLATE = "/home/ubuntu/flink-wordcount/flink-conf.yaml.template"
FLINK_CONF = "/home/ubuntu/flink-1.8.1/conf/flink-conf.yaml"

HADOOP_CONF_TEMPLATE = "/home/ubuntu/flink-wordcount/core-site.xml.template"
HADOOP_CONF= "/home/ubuntu/hadoop-3.1.2/etc/hadoop/core-site.xml"

WORKERS = "/home/ubuntu/workers.txt"
HADOOP_SLAVES = "/home/ubuntu/hadoop-3.1.2/etc/hadoop/workers"
FLINK_SLAVES = "/home/ubuntu/flink-1.8.1/conf/slaves"


def format_config(master_ip, conf_template, conf):
    with open(conf_template, 'r') as f:
        template = f.read()
    template = template.format(master_ip=master_ip)
    with open(conf, 'w+') as f:
        f.write(template)

def main(master_ip, num_nodes):
    format_config(master_ip, FLINK_CONF_TEMPLATE, FLINK_CONF)
    format_config(master_ip, HADOOP_CONF_TEMPLATE, HADOOP_CONF)

    # Write the worker IP addresses.
    workers = []
    with open(WORKERS, 'r') as f:
        for worker in f.readlines():
            workers.append(worker)
    assert len(workers) > num_nodes
    with open(FLINK_SLAVES, 'w+') as f:
        # Skip the master node for flink's jobmanager.
        for worker in workers[1:num_nodes + 1]:
            f.write(worker)

    with open(HADOOP_SLAVES, 'w+') as f:
        for worker in workers:
            f.write(worker)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Format configs.')
    parser.add_argument(
            '--master-ip',
            type=str,
            required=True)
    parser.add_argument(
            '--num-nodes',
            type=int,
            required=True)
    args = parser.parse_args()

    main(args.master_ip, args.num_nodes)
