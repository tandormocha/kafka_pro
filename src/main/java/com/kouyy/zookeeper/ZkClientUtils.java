package com.kouyy.zookeeper;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.zookeeper.data.ACL;
import org.springframework.stereotype.Component;

@Component
public class ZkClientUtils {
    private static ZookeeperClient zkClient;
    private static int timeOut=30;

    /**
     * 单例模式
     * @param zkAddr
     * @param namespace
     * @param acl
     * @return
     * @throws Exception
     */
    public static ZookeeperClient getZkClient(String zkAddr,String namespace,ACL acl) throws Exception{
        if(zkClient!=null){
            return zkClient;
        }
        synchronized (ZkClientUtils.class) {
            if(null != zkClient){
                return zkClient;
            }
            zkClient = new ZookeeperClient(zkAddr,timeOut, namespace, acl);
        }
        return zkClient;
    }

    /*******************************************************************************************/
    public static String address = "172.20.15.220:2181,172.20.15.221:2181,172.20.15.222:2181";
    public static void main(String args[]){

        testDistributeLock();
//        testCreateNode();
    }


    public static void testCreateNode(){
        try {
            ZookeeperClient zkClient = ZkClientUtils.getZkClient(address,"ns", null);
            zkClient.createPersitentNode("/test/node", "data", true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testDistributeLock(){

        for(int i=0;i<50;i++){
            new Thread(){
                @Override
                public void run() {
                    InterProcessLock lock = null;
                    try{
                        ZookeeperClient zkClient = ZkClientUtils.getZkClient(address,"dislock", null);
                        lock = zkClient.getInterProcessLock("/distributeLock");
                        System.out.println(Thread.currentThread().getName()+"申请锁");
                        lock.acquire();
                        System.out.println(Thread.currentThread().getName()+"持有锁");
                        Thread.sleep(500);
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                    finally{
                        if(null != lock){
                            try {
                                lock.release();
                                System.out.println(Thread.currentThread().getName()+"释放有锁");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }

            }.start();

        }
    }
}
