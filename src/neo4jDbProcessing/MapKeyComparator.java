package neo4jDbProcessing;

import java.util.Comparator;

public class MapKeyComparator implements Comparator< Integer> {
	public int compare(Integer str1, Integer str2) {

        return str1.compareTo(str2);
    }
}
