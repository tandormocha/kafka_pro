package com.kouyy.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.TimeUnit;


public class ZookeeperClient {

    private String zkAddr;
    private int timeOut;
    private String authSchema;
    private String authInfo;
    private CuratorFramework client;    //zookeeper客户端


    public ZookeeperClient(String zkAddr, int timeOut, String namespace) throws Exception {

        this(zkAddr, timeOut, namespace, null);

    }

    /**
     * 获取zk 连接客户端
     *
     * @param zkAddr    zk地址 ip:port,ip:port,ip:port
     * @param timeOut   连接超时ms
     * @param namespace 所有的操作都是在 /namespace 下的节点操作
     * @param acl       Access Control List（访问控制列表）。Znode被创建时带有一个ACL列表<br>
     *                  acl 主要由三个维度：schema,id,permision 控制节点权限 <br>
     *                  eg:<br>
     *                  Id id = new Id("digest", DigestAuthenticationProvider.generateDigest("username:password"));<br>
     *                  ACL acl = new ACL(ZooDefs.Perms.ALL, id); <br>
     *                  <br>
     *                  维度 schema: <br>
     *                  1：digest 用户名+密码验证 它对应的维度id=username:BASE64(SHA1(password))<br>
     *                  2：host 客户端主机名hostname验证 <br>
     *                  3：ip 它对应的维度id=客户机的IP地址，设置的时候可以设置一个ip段，比如ip:192.168.1.0/16, 表示匹配前16个bit的IP段<br>
     *                  4：auth 使用sessionID验证 <br>
     *                  5：world 无验证，默认是无任何权限  它下面只有一个id, 叫anyone  <br>
     *                  6：super: 在这种scheme情况下，对应的id拥有超级权限，可以做任何事情(cdrwa)  <br>
     *                  7：sasl: sasl的对应的id，是一个通过了kerberos认证的用户id  <br>
     *                  <br>
     *                  维度：permision <br>
     *                  ZooDefs.Perms.READ 读权限<br>
     *                  ZooDefs.Perms.WRITE 写权限<br>
     *                  ZooDefs.Perms.CREATE 创建节点权限<br>
     *                  ZooDefs.Perms.DELETE 删除节点权限<br>
     *                  ZooDefs.Perms.ADMIN 能设置权限<br>
     *                  ZooDefs.Perms.ALL 所有权限<br>
     *                  ALL = READ | WRITE | CREATE | DELETE | ADMIN<br>
     * @throws Exception
     */

    public ZookeeperClient(String zkAddr, int timeOut, String namespace, ACL acl) throws Exception {
        this.zkAddr = zkAddr;
        if (timeOut > 0) {
            this.timeOut = timeOut;
        }
        if (null != acl) {
            this.authSchema = acl.getId().getScheme();
            this.authInfo = acl.getId().getId();
        }
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory
                .builder().connectString(this.zkAddr).namespace(StringUtils.isEmpty(namespace) ? "" : namespace)
                .connectionTimeoutMs(this.timeOut)
                .retryPolicy(new RetryNTimes(5, 10));
        if ((!StringUtils.isBlank(this.authSchema))
                && (!StringUtils.isBlank(this.authInfo))) {
            builder.authorization(this.authSchema, this.authInfo.getBytes());
        }
        System.out.println("namespace:"+namespace);
        this.client = builder.build();
        this.client.start();
        this.client.blockUntilConnected(5, TimeUnit.SECONDS);

    }


    /**
     * 创建一个所有权限节点即schema:world;id:annyone;permision:ZooDefs.Perms.ALL
     *
     * @param nodePath   创建的结点路径
     * @param data       节点数据
     * @param createMode 节点模式
     * @param recursion  当父目录不存在是否创建 true:创建，fasle:不创建
     * @throws Exception
     */
    public void createNode(String nodePath, String data, CreateMode createMode, boolean recursion)
            throws Exception {

        createNode(nodePath, ZooDefs.Ids.OPEN_ACL_UNSAFE, data, createMode, recursion);
    }


    /**
     * 创建节点
     *
     * @param nodePath   创建节点的路径
     * @param acls       节点控制权限列表
     * @param data       节点存放的数据
     * @param createMode 创建节点的模式
     * @param recursion  当父目录不存在是否创建 true:创建，fasle:不创建
     *                   节点模式CreateMode<br>
     *                   1：CreateMode.EPHEMERAL 创建临时节点；该节点在客户端掉线的时候被删除<br>
     *                   2：CreateMode.EPHEMERAL_SEQUENTIAL  临时自动编号节点，一旦创建这个节点的客户端与服务器端口也就是session 超时，这种节点会被自动删除，并且根据当前已近存在的节点数自动加 1，然后返回给客户端已经成功创建的目录节点(可做分布式锁)<br>
     *                   3：CreateMode.PERSISTENT 持久化目录节点，存储的数据不会丢失。<br>
     *                   4：CreateMode.PERSISTENT_SEQUENTIAL  顺序自动编号的持久化目录节点，存储的数据不会丢失，并且根据当前已近存在的节点数自动加 1，然后返回给客户端已经成功创建的目录节点名<br>
     * @throws Exception
     */
    public void createNode(String nodePath, List<ACL> acls, String data,
                           CreateMode createMode, boolean recursion) throws Exception {
        byte[] bytes = null;
        if (!StringUtils.isBlank(data)) {
            bytes = data.getBytes("UTF-8");
        }
        createNode(nodePath, acls, bytes, createMode, recursion);
    }

    /**
     * @param nodePath   创建节点的路径
     * @param acls       节点控制权限列表
     * @param data       节点存放的数据
     * @param createMode 创建节点的模式
     * @param recursion  当父目录不存在是否创建 true:创建，fasle:不创建
     *                   节点模式CreateMode<br>
     *                   1：CreateMode.EPHEMERAL 创建临时节点；该节点在客户端掉线的时候被删除<br>
     *                   2：CreateMode.EPHEMERAL_SEQUENTIAL  临时自动编号节点，一旦创建这个节点的客户端与服务器端口也就是session 超时，这种节点会被自动删除，并且根据当前已近存在的节点数自动加 1，然后返回给客户端已经成功创建的目录节点(可做分布式锁)<br>
     *                   3：CreateMode.PERSISTENT 持久化目录节点，存储的数据不会丢失。<br>
     *                   4：CreateMode.PERSISTENT_SEQUENTIAL  顺序自动编号的持久化目录节点，存储的数据不会丢失，并且根据当前已近存在的节点数自动加 1，然后返回给客户端已经成功创建的目录节点名<br>
     * @throws Exception
     */
    public void createNode(String nodePath, List<ACL> acls, byte[] data,
                           CreateMode createMode, boolean recursion) throws Exception {
        if (recursion) {
            ((BackgroundPathAndBytesable<?>) ((ACLBackgroundPathAndBytesable<?>) this.client
                    .create().creatingParentsIfNeeded().withMode(createMode))
                    .withACL(acls)).forPath(nodePath, data);
        } else {
            ((BackgroundPathAndBytesable<?>) ((ACLBackgroundPathAndBytesable<?>) this.client
                    .create().withMode(createMode))
                    .withACL(acls)).forPath(nodePath, data);
        }
    }

    /**
     * 创建一个所有权限的永久节点
     *
     * @param nodePath
     * @param data
     * @param recursion 当父目录不存在是否创建 true:创建，fasle:不创建
     * @throws Exception
     */
    public void createPersitentNode(String nodePath, String data, boolean recursion) throws Exception {

        createNode(nodePath, data, CreateMode.PERSISTENT, recursion);
    }

    /**
     * 创建一个所有权限的零时节点
     *
     * @param nodePath
     * @param data
     * @param recursion 当父目录不存在是否创建 true:创建，fasle:不创建
     * @throws Exception
     */
    public void createEphemeralNode(String nodePath, String data, boolean recursion) throws Exception {

        createNode(nodePath, data, CreateMode.EPHEMERAL, recursion);
    }

    /**
     * 创建一个带权限的永久节点
     *
     * @param nodePath
     * @param data
     * @param recursion 当父目录不存在是否创建 true:创建，fasle:不创建
     * @throws Exception
     */
    public void createPersitentNodeWithAcl(String nodePath, String data, List<ACL> acls, boolean recursion) throws Exception {

        createNode(nodePath, acls, data, CreateMode.PERSISTENT, recursion);
    }

    /**
     * 创建一个带权限的临时节点
     *
     * @param nodePath
     * @param data
     * @param recursion 当父目录不存在是否创建 true:创建，fasle:不创建
     * @throws Exception
     */
    public void createEphemeralNodeAcl(String nodePath, String data, List<ACL> acls, boolean recursion) throws Exception {

        createNode(nodePath, acls, data, CreateMode.EPHEMERAL, recursion);
    }


    /**
     * 创建序列节点且当父节点不存在时创建父节点
     *
     * @param nodePath
     * @param acls       可参考：ZooDefs.Ids
     * @param createMode
     * @param recursion  当父目录不存在是否创建 true:创建，fasle:不创建
     * @throws Exception
     */
    public void createSeqNode(String nodePath, List<ACL> acls, CreateMode createMode, boolean recursion) throws Exception {
        if (recursion) {
            ((BackgroundPathAndBytesable<?>) ((ACLBackgroundPathAndBytesable<?>) this.client
                    .create().creatingParentsIfNeeded()
                    .withMode(createMode))
                    .withACL(acls)).forPath(nodePath);
        } else {
            ((BackgroundPathAndBytesable<?>) ((ACLBackgroundPathAndBytesable<?>) this.client
                    .create()
                    .withMode(createMode))
                    .withACL(acls)).forPath(nodePath);
        }
    }

    /**
     * 存在返回 节点stat 信息；否则返回null
     *
     * @param path
     * @return
     * @throws Exception
     */
    public Stat exists(String path) throws Exception {

        return this.client.checkExists().forPath(path);
    }

    /**
     * 判断节点是否存在，存在则注册节点监视器
     *
     * @param path
     * @param watcher
     * @return
     */
    public boolean exists(String path, Watcher watcher) throws Exception {

        if (null != watcher) {
            return null != ((BackgroundPathable<?>) this.client.checkExists().usingWatcher(watcher)).forPath(path);
        }
        return null != this.client.checkExists().forPath(path);
    }

    /**
     * 判断是否处于连接状态
     *
     * @return
     */
    public boolean isConnected() {

        if ((null == this.client)
                || (!CuratorFrameworkState.STARTED.equals(this.client
                .getState()))) {
            return false;
        }
        return true;
    }

    public void retryConnection() {
        this.client.start();
    }

    /**
     * 获取连接客户端
     *
     * @return
     */
    public CuratorFramework getInnerClient() {

        return this.client;

    }

    /**
     * 关闭连接
     */
    public void quit() {

        if ((null != this.client)
                && (CuratorFrameworkState.STARTED
                .equals(this.client.getState()))) {
            this.client.close();
        }
    }


    /**
     * 删除节点
     *
     * @param path
     * @param deleChildren
     * @throws Exception
     */
    public void deleteNode(String path, boolean deleChildren) throws Exception {

        if (deleChildren) {
            this.client.delete().guaranteed().deletingChildrenIfNeeded()
                    .forPath(path);
        } else {
            this.client.delete().forPath(path);
        }
    }

    /**
     * 设置节点数据
     *
     * @param nodePath
     * @param data
     * @throws Exception
     */
    public void setNodeData(String nodePath, String data) throws Exception {

        byte[] bytes = null;
        if (!StringUtils.isBlank(data)) {
            bytes = data.getBytes("UTF-8");
        }
        setNodeData(nodePath, bytes);
    }

    /**
     * 设置节点数据
     *
     * @param nodePath
     * @param data
     * @throws Exception
     */
    public void setNodeData(String nodePath, byte[] data) throws Exception {
        this.client.setData().forPath(nodePath, data);
    }

    public String getNodeData(String nodePath, boolean watch) throws Exception {
        byte[] data;
        if (watch) {
            data = (byte[]) ((BackgroundPathable<?>) this.client.getData()
                    .watched()).forPath(nodePath);
        } else {
            data = (byte[]) this.client.getData().forPath(nodePath);
        }
        if ((null == data) || (data.length <= 0)) {
            return null;
        }
        return new String(data, "UTF-8");
    }

    public String getNodeData(String nodePath) throws Exception {
        return getNodeData(nodePath, false);
    }

    public String getNodeData(String nodePath, Watcher watcher)
            throws Exception {
        byte[] data = getNodeBytes(nodePath, watcher);
        return new String(data, "UTF-8");
    }

    public byte[] getNodeBytes(String nodePath, Watcher watcher)
            throws Exception {
        byte[] bytes = null;
        if (null != watcher) {
            bytes = (byte[]) ((BackgroundPathable<?>) this.client.getData()
                    .usingWatcher(watcher)).forPath(nodePath);
        } else {
            bytes = (byte[]) this.client.getData().forPath(nodePath);
        }
        return bytes;
    }

    public byte[] getNodeBytes(String nodePath) throws Exception {
        return getNodeBytes(nodePath, null);
    }


    @SuppressWarnings("unchecked")
    public List<String> getChildren(String nodePath, Watcher watcher)
            throws Exception {
        return (List<String>) ((BackgroundPathable<?>) this.client
                .getChildren().usingWatcher(watcher)).forPath(nodePath);
    }

    public List<String> getChildren(String path) throws Exception {
        return (List<String>) this.client.getChildren().forPath(path);
    }

    @SuppressWarnings("unchecked")
    public List<String> getChildren(String path, boolean watcher)
            throws Exception {
        if (watcher) {
            return (List<String>) ((BackgroundPathable<?>) this.client
                    .getChildren().watched()).forPath(path);
        }
        return (List<String>) this.client.getChildren().forPath(path);
    }


    public ZookeeperClient addAuth(String authSchema, String authInfo)
            throws Exception {
        synchronized (ZookeeperClient.class) {
            this.client.getZookeeperClient().getZooKeeper()
                    .addAuthInfo(authSchema, authInfo.getBytes());
        }
        return this;
    }

    /**
     * 分布式锁
     *
     * @param lockPath
     * @return
     */
    public InterProcessLock getInterProcessLock(String lockPath) {
        return new InterProcessMutex(this.client, lockPath);
    }

}