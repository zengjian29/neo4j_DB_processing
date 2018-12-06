package neo4jDbProcessing;

import java.util.Map;
import java.util.TreeMap;

public class MapSortDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<Integer, Integer> map = new TreeMap<Integer, Integer>();

        map.put(1, 3);
        map.put(4, 55);
        map.put(2, 33);
        map.put(7, 22);
        map.put(9, 22);
        map.put(7, 26);
        map.put(0, 58);
        map.put(6, 56);

        Map<Integer, Integer> resultMap = sortMapByKey(map);  //按Key进行排序

        for (Map.Entry<Integer, Integer> entry : resultMap.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
	}
	/**
     * 使用 Map按key进行排序
     * @param map
     * @return
     */
    public static Map<Integer, Integer> sortMapByKey(Map<Integer, Integer> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<Integer, Integer> sortMap = new TreeMap<Integer, Integer>(
                new MapKeyComparator0());

        sortMap.putAll(map);

        return sortMap;
    }

}
