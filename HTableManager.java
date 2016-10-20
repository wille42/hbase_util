import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

/**
 * HBase table util
 *
 * @author William
 *
 */
public class HTableManager {

	public static final byte[] DEFAULT_FAMILY_NAME = Bytes.toBytes("f1");

	private static final int MAX_FILE_SIZE = 1024 * 1024 * 256;

	private static final String[] PARTITIONS = generatPartitionSeed();

	/**
	 * 生成3844个分区种子
	 *
	 * @return String[]
	 */
	public static String[] generatPartitionSeed() {
		List<Character> seeds = Lists.newArrayList();
		for (int i = '0'; i <= '9'; i++) {
			seeds.add((char) i);
		}
		for (int i = 'A'; i <= 'Z'; i++) {
			seeds.add((char) i);
		}
		for (int i = 'a'; i <= 'z'; i++) {
			seeds.add((char) i);
		}
		int k = 0;
		String[] partions = new String[seeds.size() * seeds.size()];
		for (int i = 0; i < seeds.size(); i++) {
			for (int j = 0; j < seeds.size(); j++) {
				partions[k] = StringUtils.join(seeds.get(i), seeds.get(j));
				k++;
			}
		}
		return partions;
	}

	/**
	 * 按指定数量生成分区种子
	 *
	 * @param limit
	 * @return String[]
	 */
	public static String[] generatPartitionSeed(int limit) {
		int size = PARTITIONS.length;
		int[] space = new int[limit];
		for (int pt = 0; pt < size;) {
			for (int j = 0; j < space.length; j++) {
				++space[j];
				pt++;
				if (pt == size) {
					break;
				}
			}
		}
		String[] seed = new String[limit + 1];
		int position = 0;
		for (int i = 0; i < space.length; i++) {
			seed[i] = PARTITIONS[position];
			position += space[i];
		}
		seed[seed.length - 1] = PARTITIONS[PARTITIONS.length - 1];
		return seed;
	}

	public static String generatRowkey(String str) {
		if (str == null) {
			return "";
		}
		int i = Math.abs(str.hashCode() % PARTITIONS.length);
		return StringUtils.join(PARTITIONS[i], "-", str);
	}

	public static byte[] generatByteRowkey(String str) {
		int i = Math.abs(str.hashCode() % PARTITIONS.length);
		return Bytes.toBytes(StringUtils.join(PARTITIONS[i], "-", str));
	}

	private static HTableDescriptor getHTableDescriptor(String tableName) {
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(HTableManager.DEFAULT_FAMILY_NAME);
		columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
		columnDescriptor.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
		// columnDescriptor.setTimeToLive(60 * 60 * 24 * 365 * 1);
		columnDescriptor.setBlockCacheEnabled(true);
		columnDescriptor.setDataBlockEncoding(DataBlockEncoding.NONE);

		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		desc.setCompactionEnabled(true);
		desc.setMaxFileSize(MAX_FILE_SIZE);
		desc.addFamily(columnDescriptor);
		return desc;
	}

	/**
	 * 创建表
	 *
	 * @param tableName
	 *            表名
	 * @param partitionSeedLimit
	 *            预分区数量，最大数量3844
	 * @throws Exception
	 */
	public static void createHBaseTable(String tableName, int partitionSeedLimit) throws Exception {
		if (partitionSeedLimit > 3844 || partitionSeedLimit < 1) {
			throw new IllegalArgumentException("PartitionSeedLimit must be > 0 and < 3844.");
		}
		System.out.println("init HBase admin...");
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.tableExists(tableName)) {
			if (!admin.isTableDisabled(tableName)) {
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
			System.out.println(String.format("Table is exist, drop table %s successed.", tableName));
		}

		System.out.println(String.format("Creating HBase table %s, partition seed limit %d.", tableName, partitionSeedLimit));
		admin.createTable(getHTableDescriptor(tableName), Bytes.toByteArrays(generatPartitionSeed(partitionSeedLimit)));
		System.out.println(String.format("HBase table %s is created.", tableName));
		admin.close();
		System.out.println("==============================================");
	}

}