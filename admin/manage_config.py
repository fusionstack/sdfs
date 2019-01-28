#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os, sys, re
from utils import exec_shell, Exp, dmsg, derror
from Ganesha.config_editor import BLOCK, ArgError
from Ganesha.ganesha_mgr_utils import ExportMgr
import logging, pprint
import argparse
import errno

from utils import _check_config

#logging.basicConfig(level=logging.DEBUG)

SERVICE = 'org.ganesha.nfsd'
CONF_PATH = "/etc/ganesha/ganesha.conf"

def status_message(status, errormsg):
    print "Returns: status = %s, %s" % (str(status), errormsg)

def modify_file(filename, data):
    from tempfile import NamedTemporaryFile
    f = NamedTemporaryFile(dir=os.path.dirname(filename), delete=False)
    f.write(data)
    f.flush()
    os.fsync(f.fileno())

    # If filename exists, get its stats and apply them to the temp file
    try:
        stat = os.stat(filename)
        os.chown(f.name, stat.st_uid, stat.st_gid)
        os.chmod(f.name, stat.st_mode)
    except:
        pass

    os.rename(f.name, filename)

def get_content_as_list():
    delimeter = "-------------------------------------------------------------\n"
    list_record = []
    _exec = "/opt/sdfs/app/bin/sdfs.share -l -p nfs"
    try:
        result, err = exec_shell(_exec, p=True, need_return=True, timeout=60)
    except Exception as e:
        raise Exp(e.errno, str(e))

    list_record = result.split(delimeter)
    return list_record

def get_export(list_record):
    export_list = []
    export = {}
    client_pair = {}
    access_pair = {}
    client_list = {}
    if len(list_record) == 0:
        return export_list
    for record in list_record:
        if len(record) == 0:
            continue
        export_path = record.split('\n')[1].split(':')[1].strip(' ')
        clients = record.split('\n')[5].split(':')[1].strip(' ')
        access_type = record.split('\n')[6].split(':')[1].strip(' ')
        client_pair["Clients"] = clients
        access_pair["Access_Type"] = access_type
        client_list.update(client_pair)
        client_list.update(access_pair)
        export[export_path] = client_list
        export_list.append(export)
        client_list = {}
        export = {}

    return export_list

#根据export_path获取export_id
def get_export_id_by_path(content, export_path):
    found = False
    regex = 'EXPORT.*?{.*?Export_Id.*?=(.*?);.*?Path.*?=(.*?);.*?'
    pattern = re.compile(regex, re.S)
    items = re.findall(pattern, content)
    for item in items:
        if cmp(export_path, item[1].strip(' ')) == 0:
            found = True
            return item[0].strip(' ')
    if not found:
        return '0'

def get_clients_by_path(content, export_path):
    #regex = 'EXPORT.*?{.*?Path.*?=(.*?);.*?(CLIENT.*?{.*?Clients.*?=(.*?);).*?'
    regex = 'EXPORT\s*{.*?}\s*}'
    regex_path = '.*?Path.*?=(.*?);'
    regex_cli = '.*?Clients.*?=(.*?);.*?'
    pattern = re.compile(regex, re.S)
    items = re.findall(pattern, content)
    # 解析出每一个EXPORT {}
    for export_content in items:
        # 解析出当前EXPORT BLOCK中的Path
        pattern_path = re.compile(regex_path, re.S)
        items_path = re.findall(pattern_path, export_content)

        if cmp(export_path, items_path[0].strip(' ')) == 0:
            # 解析出当前EXPORT BLOCK中所有Clients
            pattern_cli = re.compile(regex_cli, re.S)
            items_cli = re.findall(pattern_cli, export_content)
            return items_cli

def get_clients_from_share(export_list, export_path):
    share_clients = []
    for item in export_list:
        if cmp(export_path, item.keys()[0]) == 0:
            client = item.values()[0]['Clients']
            share_clients.append(client)
    return share_clients

#当前配置文件中最大ID+1
def get_max_export_id(content):
    regex = 'EXPORT.*?{.*?Export_Id.*?=(.*?);.*?'
    pattern = re.compile(regex, re.S)
    items = re.findall(pattern, content)
    if len(items) == 0:
        return '1'
    else:
        return str(int(max(items))+1).strip(' ')

def build_common_param(export_path):
    keys_list = ['Path', 'Pseudo',  'Access_Type', 'Squash', 'Disable_Acl', 'Protocols']
    values_list = [export_path, export_path, 'RO', 'No_Root_Squash', 'True', '4']

    pairs = zip(keys_list, values_list)

    return pairs

def build_fsal_block(export_id):
    export_block = []

    export_block.append('EXPORT')
    export_block.append('Export_Id')
    export_block.append(export_id)
    export_block.append('FSAL')

    return export_block

def build_fsal_param():
    #keys_list = ['Name', 'volpath']
    #values_list = ['USS', '/home']
    keys_list = ['Name']
    values_list = ['USS']

    fsal_pairs = zip(keys_list, values_list)

    return fsal_pairs

def build_export_block(export_id, host=None):
    export_block = []

    export_block.append('EXPORT')
    export_block.append('Export_Id')
    export_block.append(export_id)

    if host is not None:
        export_block.append('CLIENT')
        export_block.append('Clients')
        export_block.append(host)

    return export_block

def build_expression(export_id):
    exp = "EXPORT(Export_ID=%s)" % (export_id)
    return exp

def del_conf(abs_conf_path, params_list):
    export_set = set()
    _exec = "cat %s | grep Path" % (CONF_PATH)

    try:
        result, err = exec_shell(_exec, p=True, need_return=True, timeout=60)
    except Exception as e:
        raise Exp(e.errno, str(e))

    with open(abs_conf_path, 'r') as fp:
        content = fp.read()
#去重
    for export in params_list:
        export_path_uss = export.keys()[0].strip(' ')
        export_set.add(export_path_uss)

    path_list = result.split('\n')
    for l_path in path_list:
        path = l_path.split('=')
        if len(path) == 2:
            export_path_conf =  path[1].split(';')[0].strip(' ')
            export_id = get_export_id_by_path(content, export_path_conf)
            # 判断配置文件中的path是否share中也存在
            if export_path_conf not in export_set:
                delete_export_by_id(content, export_id)
                exportmgr = ExportMgr(SERVICE, '/org/ganesha/nfsd/ExportMgr', 'org.ganesha.nfsd.exportmgr')
                status, msg = exportmgr.RemoveExport(export_id)
                status_message(status, msg)
                continue

            # 判断配置文件中的client是否share中也存在
            share_clients = get_clients_from_share(params_list, export_path_conf)
            conf_clients = get_clients_by_path(content, export_path_conf)
            for c_client in conf_clients:
                c_Cli = c_client.strip(' ')
                if c_Cli not in share_clients:
                    delete_client_by_host(content, export_id, c_Cli)
                    exportmgr = ExportMgr(SERVICE, '/org/ganesha/nfsd/ExportMgr', 'org.ganesha.nfsd.exportmgr')
                    export_expression = build_expression(export_id)
                    status, msg = exportmgr.UpdateExport(abs_conf_path, export_expression)
                    status_message(status, msg)

#获取export_block: ['Export', 'Export_Id', '1', 'CLIENT', 'clients', '192.168.1.1']
#获取opairs: [('Clents', ' 192.168.1.2'), ('Access_Type', ' read-only')]
def set_conf(abs_conf_path, params_list):
    is_new = False
    client_pairs = []
    export_block = []
    fsal_block = []
    client_block = []
    l_keys = []
    l_values = []
    for export in params_list:
        with open(abs_conf_path, 'r') as fp:
            content = fp.read()
        keys = export.values()[0].keys()[1]
        values = export.values()[0].values()[1]
        if cmp(values.strip(' '), "read-write") == 0:
            l_values.append("RW")
        else:
            l_values.append("RO")
        l_keys.append(keys)
        host = export.values()[0].values()[0].strip(' ')
        client_pairs = zip(l_keys, l_values)
        export_path = export.keys()[0].strip(' ')
        export_id = get_export_id_by_path(content, export_path)
        if export_id == '0':
            is_new = True
            export_id = get_max_export_id(content)

        logging.debug("export_id : %s", export_id)
#无论是否是新export记录，首先需要设置默认参数
        common_block = build_export_block(export_id)
        logging.debug("common_block : %s ", common_block)
        c_block = BLOCK(common_block)
        common_pairs = build_common_param(export_path)
        logging.debug("common_pairs : %s ", common_pairs)
        try:
            common = c_block.set_keys(content, common_pairs)
        except ArgError as e:
            sys.exit(e.error)
        modify_file(abs_conf_path, common)

#构建fsal block
        fsal_block = build_fsal_block(export_id)
        logging.debug("fsal_block : %s", fsal_block)
        fs_block = BLOCK(fsal_block)
        fsal_pairs = build_fsal_param()
        with open(abs_conf_path, 'r') as fp:
            content = fp.read()
        logging.debug("fsal_pairs : %s", fsal_pairs)
        try:
            fsal = fs_block.set_keys(content, fsal_pairs)
        except ArgError as e:
            sys.exit(e.error)
        modify_file(abs_conf_path, fsal)

#构建client block
        client_block = build_export_block(export_id, host)
        logging.debug("client_block : %s", client_block)
        cl_block = BLOCK(client_block)
        with open(abs_conf_path, 'r') as fp:
            update_content = fp.read()
        try:
            logging.debug("client_pairs : ", client_pairs)
            client = cl_block.set_keys(update_content, client_pairs)
        except ArgError as e:
            sys.exit(e.error)
        modify_file(abs_conf_path, client)

        common_pairs = []
        fsal_pairs = []
        client_pairs = []
        l_keys = []
        l_values = []

        common_block = []
        fsal_block = []
        client_block = []
        is_new = False
        host = ""

def delete_export_by_id(content, export_id):
    export_block = build_export_block(export_id)
    block = BLOCK(export_block)

    try:
        new = block.del_keys(content, [])
    except ArgError as e:
        sys.exit(e.error)
    modify_file(CONF_PATH, new)

def delete_client_by_host(content, export_id, host):
    if host is None:
        dmsg("error : must specify host value ")
        sys.exit(-1)
    export_block = build_export_block(export_id, host)
    block = BLOCK(export_block)

    try:
        new = block.del_keys(content, [])
    except ArgError as e:
        sys.exit(e.error)
    modify_file(CONF_PATH, new)


def disable_nfs3():
    _check_config("/etc/ganesha/ganesha.conf", "NFS_Core_Param", " ", '{NFS_Protocols = "4";}', 1)
    
    
#  通过是否可以导出目录，
#  判断nfs是否正常启动
def nfs_running():
    _exec = 'showmount -e localhost'
    retry = 3

    while (retry > 0):
        try:
            retry = retry - 1
            exec_shell(_exec, need_return=True, timeout=10)
            return True
        except Exp, e:
            derror("%s : %s\n" % (_exec, str(e)))
            return False

def enable_export():
    if not nfs_running():
        derror('nfs-ganesha is not running')
        sys.exit(errno.ENOENT)

    _exec = "pkill -1 ganesha.nfsd"
    try:
        exec_shell(_exec, timeout=10)
        return True
    except Exp, e:
        derror("%s : %s\n" % (_exec, str(e)))
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    def _load(args):
        list_record = get_content_as_list()
        export_list = get_export(list_record)

        set_conf(CONF_PATH, export_list)
        del_conf(CONF_PATH, export_list)
        disable_nfs3()
        #enable_export()
    parser_load = subparsers.add_parser('load', help='load config')
    parser_load.set_defaults(func=_load)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)
