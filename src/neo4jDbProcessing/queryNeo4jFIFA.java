package neo4jDbProcessing;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class queryNeo4jFIFA {
	static String url = "jdbc:neo4j:http://localhost:7474/browser/";
	static String database = "neo4j";
	static String passward = "666";
	static ArrayList<String> resGraph = new ArrayList<String>();
	static ArrayList<String> resLabels = new ArrayList<String>();
	static ArrayList<String> midListN = new ArrayList<String>();
	static ArrayList<String> labelListN = new ArrayList<String>();
	static ArrayList<String> typerListNM = new ArrayList<String>();
	static ArrayList<String> midListM = new ArrayList<String>();
	static ArrayList<String> labelListM = new ArrayList<String>();
	static ArrayList<String> midList = new ArrayList<String>();
	static ArrayList<String> labelList = new ArrayList<String>();
	static ArrayList<String> typerList = new ArrayList<String>();
	static HashMap<String, String> MidLabelmap = new HashMap<String, String>();
	static HashMap<String, String> nodeLabelmap = new HashMap<String, String>();
	static HashMap<String, String> edgeLabelmap = new HashMap<String, String>();
	static HashMap<String, ArrayList<String>> resMap = new HashMap<String, ArrayList<String>>();
	static Map<Integer, Integer> beMidLabelMap = new HashMap<Integer, Integer>();
	static Map<Integer, Integer> sortedMidLabelMap = new HashMap<Integer, Integer>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Query Neo4jDB!");
		// StopWatch watch = new StopWatch();
		queryNeo4jFIFA.queryAction();
		queryNeo4jFIFA.writeTxt(midList, labelList, typerList);
		queryNeo4jFIFA.writeMapTxt(nodeLabelmap, edgeLabelmap);
		// queryNeo4j.matchString();
	}

	public static void queryAction() {
		try {
			// watch.start();
			String queryGraph = "MATCH (n)-[r]->(m) RETURN id(n),labels(n), id(m),labels(m),type(r)";
			Connection con = DriverManager.getConnection(url, database, passward);
			Statement stmt = con.createStatement();
			ResultSet reGraph = stmt.executeQuery(queryGraph);
			while (reGraph.next()) {
				String midN = reGraph.getString("id(n)");
				String labelsN = reGraph.getString("labels(n)");
				String midM = reGraph.getString("id(m)");
				String labelsM = reGraph.getString("labels(m)");
				String typeR = reGraph.getString("type(r)");
				// 将查询的数据存入list中
				midListN.add(midN);
				labelListN.add(labelsN);
				midListM.add(midM);
				labelListM.add(labelsM);
				typerListNM.add(typeR);
				// 判断mid、label和typr(r)是否存在list中
				ifExist(midN, labelsN);
				ifExist(midM, labelsM);
				ifExistType(typeR);
			}

			System.out.println("midListN:" + midListN);
			System.out.println("labelListN:" + labelListN);
			System.out.println("midListM:" + midListM);
			System.out.println("labelListM:" + labelListM);
			System.out.println("typerListNM:" + typerListNM);

			System.out.println("midList:" + midList);
			System.out.println("labelList:" + labelList);
			System.out.println("typerList:" + typerList);
			// System.out.println("sortedMidLabelMap:"+sortedMidLabelMap);
			for(int i=0;i<labelList.size();i++){
				nodeLabelmap.put(String.valueOf(i), labelList.get(i));
			}
			for(int i=0;i<typerList.size();i++){
				edgeLabelmap.put(String.valueOf(i), typerList.get(i));
			}
			System.out.println("nodeLabelmap:" + nodeLabelmap);
			System.out.println("edgeLabelmap:" + edgeLabelmap);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void writeTxt(ArrayList<String> midList, ArrayList<String> labelList, ArrayList<String> typerList) {
		FileWriter fw;
		try {
			String fName = "FIFA.txt";
			fw = new FileWriter(fName);
			// 将节点写入txt文件
			fw.write("t # " + ":\n");
			Set<String> midLableKeys = MidLabelmap.keySet();
			for (String midLabelKey : midLableKeys) {
				int indexVertex = midList.indexOf(midLabelKey);// 获取序号
				int indexLabel = labelList.indexOf(MidLabelmap.get(midLabelKey));
				beMidLabelMap.put(indexVertex, indexLabel);
			}
			sortedMidLabelMap = sortMapByKey(beMidLabelMap); // 对map中key值进行排序
			Set<Integer> imidLableKeys = beMidLabelMap.keySet();
			for (Integer imidLableKey : imidLableKeys) {
				int inVertex = imidLableKey;
				int inLabel = beMidLabelMap.get(imidLableKey);
				String out = serializeVertex(inVertex, inLabel);
				fw.write(out);
			}
			// 将边写入txt文件
			for (int i = 0; i < midListN.size(); i++) {
				int indexNvertex = midList.indexOf(midListN.get(i));// 将边type(r)的String类型转换成int，获取序号
				int indexMvertex = midList.indexOf(midListM.get(i));
				int indexEdgelabel = typerList.indexOf(typerListNM.get(i));
				String out = serializeEdge(indexNvertex, indexMvertex, indexEdgelabel);

				fw.write(out);
			}
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void writeMapTxt(HashMap<String, String> nodeLabelMap, HashMap<String, String> edgeLabelMap) {
		FileWriter fw;
		try {
			String fName = "Label.txt";
			fw = new FileWriter(fName);
			// 将节点写入txt文件
			fw.write("Labels correspondence " + ":\n");
			Set<String> nodeLabelKeys = nodeLabelMap.keySet();
			fw.write("-------------------nodeLabels-------------------"+"\n");
			for (String nodeLabelKey : nodeLabelKeys) {
				fw.write(nodeLabelKey+":");
				fw.write(nodeLabelMap.get(nodeLabelKey)+"\n");
			}	
			Set<String> edgeLabelKeys = edgeLabelMap.keySet();
			fw.write("-------------------edgeLabels-------------------"+"\n");
			for (String edgeLabelKey : edgeLabelKeys) {
				fw.write(edgeLabelKey+":");
				fw.write(edgeLabelMap.get(edgeLabelKey)+"\n");
			}	
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Map<Integer, Integer> sortMapByKey(Map<Integer, Integer> map) {
		if (map == null || map.isEmpty()) {
			return null;
		}
		Map<Integer, Integer> sortMap = new TreeMap<Integer, Integer>(new MapKeyComparator());
		sortMap.putAll(map);
		return sortMap;
	}

	public static String serializeVertex(int i, int j) {
		String text = "";
		text += "v " + i + " " + j + "\n";
		return text;
	}

	public static String serializeEdge(int a, int b, int c) {
		String text = "";
		text += "e " + a + " " + b + " " + c + "\n";
		return text;
	}

	public static void ifExist(String mid, String label) {
		// 判断是否存在list中
		if (midList.isEmpty()) {
			midList.add(mid);
			MidLabelmap.put(mid, label);// 存放mid和label的mapping关系
		} else {
			if (mid != null) {
				int count = 0;
				for (int i = 0; i < midList.size(); i++) {
					if (midList.get(i).equals(mid)) {
						System.out.println("mid exist!");
						break;
					}
					count++;
				}
				if (count == midList.size()) {
					midList.add(mid);
					MidLabelmap.put(mid, label);
				}
			}
		}
		if (labelList.isEmpty()) {
			labelList.add(label);
		} else {
			if (label != null) {
				int count = 0;
				for (int i = 0; i < labelList.size(); i++) {
					if (labelList.get(i).equals(label)) {
						System.out.println("label exist!");
						break;
					}
					count++;
				}
				if (count == labelList.size()) {
					labelList.add(label);
				}
			}
		}
	}

	public static void ifExistType(String typer) {
		// 判断是否存在type(r)
		if (typerList.isEmpty()) {
			typerList.add(typer);
		} else {
			if (typer != null) {
				int count = 0;
				for (int i = 0; i < typerList.size(); i++) {
					if (typerList.get(i).equals(typer)) {
						System.out.println("typer exist!");
						break;
					}
					count++;
				}
				if (count == typerList.size()) {
					typerList.add(typer);
				}
			}
		}
	}

	public static void matchString() {
		ArrayList<String> midList = new ArrayList<String>();
		String midA = "/m/011yd2";
		midList.add(midA);
		String midB = "/m/0cc2t3x";
		// String midB = "/m/011yd2";
		for (int i = 0; i < midList.size(); i++) {

			if (midList.get(i).equals(midB)) {
				System.out.println("Yes");
			} else {
				System.out.println("No");
				midList.add(midB);
				break;
			}
		}
		System.out.println("midList:" + midList);
	}

}