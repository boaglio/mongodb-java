/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tengen;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

public class Week3Homework1 {

	@SuppressWarnings({"unchecked","rawtypes"})
	public static void main(String[] args) throws UnknownHostException {

		MongoClient client = new MongoClient(new ServerAddress("localhost",27017));

		DB database = client.getDB("school");

		DBCollection students = database.getCollection("students");

		Set<String> colls = database.getCollectionNames();

		for (String s : colls) {
			System.out.println("collection = " + s);
		}

		System.out.println(" Total: " + students.count());

		DBCursor cursor = students.find();
		try {
			while (cursor.hasNext()) {
				DBObject resultElement = cursor.next();
				Map resultElementMap = resultElement.toMap();
				Collection<?> keySet = resultElementMap.keySet();
				Collection<?> resultValues = resultElementMap.values();

				System.out.println(keySet);
				System.out.println(resultValues);
				int id = (Integer) resultElementMap.get("_id");
				String nome = (String) resultElementMap.get("name");
				BasicDBList scores = (BasicDBList) resultElementMap.get("scores");

				System.out.println(nome);
				for (Object obj : scores) {
					BasicDBObject score = (BasicDBObject) obj;
					Map<?,?> scoreResultElementMap = score.toMap();
					System.out.println("score=" + scoreResultElementMap.get("score"));
				}
				Double lowerKey = scoreMaisBaixo(scores);
				System.out.println(" lowerKey=" + lowerKey);
				if (lowerKey != null) {
					Object objParaDel = null;
					for (Object obj : scores) {
						BasicDBObject score = (BasicDBObject) obj;
						Map scoreResultElementMap = score.toMap();
						Double scoreLido = (Double) scoreResultElementMap.get("score");
						if (scoreLido == lowerKey) {
							objParaDel = obj;
						}
					}
					scores.remove(objParaDel);
					resultElementMap.put("scores",scores);

					DBObject docDel = new BasicDBObject("_id",id);
					WriteResult res = students.remove(docDel);
					System.out.println("REMOVENDO >>>>> RES=" + res + "DOC=" + docDel);

					DBObject doc = new BasicDBObject(resultElementMap);
					res = students.insert(doc);
					System.out.println("INSERINDO   >>>>> RES=" + res + "DOC=" + doc);
				}

			}
		} finally {
			cursor.close();
		}

		System.out.println(" ================ ================ ================ ================ ================ ");
		System.out.println(" Total: " + students.count());

	}

	private static Double scoreMaisBaixo(BasicDBList scores) {

		TreeSet<Double> listaOrdenada = new TreeSet<Double>();
		for (Object obj : scores) {
			BasicDBObject score = (BasicDBObject) obj;
			Map<?,?> scoreResultElementMap = score.toMap();
			String tipoDoc = (String) scoreResultElementMap.get("type");
			if (tipoDoc.equals("homework")) {
				listaOrdenada.add((Double) scoreResultElementMap.get("score"));
			}
		}

		if (listaOrdenada.size() == 1) { return null; }

		return listaOrdenada.first();
	}
}
