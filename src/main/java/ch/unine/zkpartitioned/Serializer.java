package ch.unine.zkpartitioned;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Serializer<T> implements org.menagerie.Serializer<T>{

    public Serializer(){

    }

	public static List<byte[]> serializeACL(List<ACL> acl) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Serializer.serializeACL");
		
		List<byte[]> aclSerialized = null;
		
		if (acl != null) {
			aclSerialized = new ArrayList<byte[]>();
			 
			for (ACL a : acl) {
	
				try {
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(bos);
					a.write(oos);
					oos.flush();
					bos.flush();
					aclSerialized.add(bos.toByteArray());
					oos.close();
					bos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return aclSerialized;
	}
	
	public static List<ACL> deserializeACL(List<byte[]> aclSerialized) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Serializer.deserializeACL");
		
		List<ACL> acl = null;
		
		if (aclSerialized != null) {
			acl = new ArrayList<ACL>();
		
			for (byte[] bytes : aclSerialized) {
				try {
					ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
					ObjectInputStream ins = new ObjectInputStream(bis);
				    
					ACL a = new ACL();
					a.readFields(ins);
					acl.add(a);
					
				    bis.close();
				    ins.close();
				} catch (IOException e) {
					e.printStackTrace();
				}	   
			}
		}
		
		return acl;
	}

	public static byte[] serializeStat(Stat stat) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Serializer.serializeStat");
		
		byte[] statSerialized = null;
		
		if (stat != null) {
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				stat.write(oos);
				oos.flush();
				bos.flush();
				statSerialized = bos.toByteArray();
				oos.close();
				bos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		
		return statSerialized;
	}
	
	public static Stat deserializeStat(byte[] statSerialized) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Serializer.deserializeStat");
		
		Stat stat = new Stat();
		
		if (statSerialized != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(statSerialized);
				ObjectInputStream ins = new ObjectInputStream(bis);
				
				stat.readFields(ins);
				
			    bis.close();
			    ins.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	   
		}
		
		return stat;
	}
	
	public byte[] serialize(T obj) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Serializer.serialize");
		
		byte[] serializedCmd = null;
		
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(obj);
			oos.flush();
			serializedCmd = bos.toByteArray();
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return serializedCmd;
	}
	
	public T deserialize(byte[] bytes) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Serializer.deserialize");
		
		T logEntry = null;
		
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ObjectInputStream ins = new ObjectInputStream(bis);
		    logEntry =(T) ins.readObject();
		    bis.close();
		    ins.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return logEntry;
	}
	
	public static byte[] serializeData(Data result) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Serializer.serializeData");
		
		byte[] data = result.getData();
		byte[] dataSize = ByteBuffer.allocate(4).putInt(data.length).array();
		byte[] statSerialized = serializeStat(result.getStat());
		
		byte[] serializedData = null;
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try {
			outputStream.write(dataSize);
			outputStream.write(data);
			outputStream.write(statSerialized);
			outputStream.flush();
			serializedData = outputStream.toByteArray();
			outputStream.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		return serializedData;
	}
	
	public static Data deserializeData(byte[] dataSerialized) {
		if (ZooKeeperPartitioned.logger.isTraceEnabled())
			 ZooKeeperPartitioned.logger.trace("Serializer.deserializeData");
		
		ByteArrayInputStream inputStream = new ByteArrayInputStream(dataSerialized);
			  
		byte[] dataSize = new byte[4];
		inputStream.read(dataSize, 0, 4);
		int dataLen = new BigInteger(dataSize).intValue();
		byte[] data = new byte[dataLen];
		byte[] statSerialized = new byte[dataSerialized.length - 4 - dataLen];
		inputStream.read(data, 0, dataLen);
		inputStream.read(statSerialized, 0, dataSerialized.length - 4 - dataLen);
		
		Stat stat = deserializeStat(statSerialized);
		
		Data dataResult = new Data(data, stat);
		return dataResult;
	}
}
