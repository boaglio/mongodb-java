import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/*
 * Antes de limpar:
 * > db.images.find({"tags":"sunrises"}).count();
 * 49887
 * ************************** depois de rodar!!!!!...
 * > db.images.count();
 * 89737
 * > db.images.find({"tags":"sunrises"}).count();
 * 44787
 * >
 */
public class FinalQuestion7 {

	@SuppressWarnings({"unchecked","rawtypes"})
	public static void main(String[] args) throws UnknownHostException {

		MongoClient client = new MongoClient(new ServerAddress("localhost",27017));

		DB database = client.getDB("test");

		DBCollection imagens = database.getCollection("images");
		DBCollection albums = database.getCollection("albums");
		ArrayList<Integer> imagensOrfas = new ArrayList();

		Set<String> colls = database.getCollectionNames();

		for (String s : colls) {
			System.out.println("collection = " + s);
		}

		System.out.println(" Total: " + imagens.count());

		DBCursor cursor = imagens.find();
		int contador = 1;
		try {
			// loop por cada imagem
			while (cursor.hasNext()) {

				// se nao achar em nenhum album... remove a imagem
				DBObject resultElement = cursor.next();
				Map resultElementMap = resultElement.toMap();
				Collection<?> keySet = resultElementMap.keySet();
				Collection<?> resultValues = resultElementMap.values();

				System.out.println(keySet);
				System.out.println(resultValues);
				int idImagem = (Integer) resultElementMap.get("_id");

				DBObject album = albums.findOne(new BasicDBObject("images",idImagem));

				if (album != null) {
					System.out.println("Algum album usa a imagem!");
				} else {
					imagensOrfas.add(idImagem);
					System.out.println(">>>> NENHUM album usa a imagem!");
					imagens.remove(resultElement);
				}

				System.out.println("contador=" + contador++);
				System.out.println(" ================ ================ ================ ================ ================ ");

			}
		} finally {
			cursor.close();
		}

		System.out.println(" ================ ================ ================ ================ ================ ");
		System.out.println("Imagens sem album:" + imagensOrfas.size());
		System.out.println("Imagens total:" + contador);
		System.out.println("Imagens diff:" + (contador - imagensOrfas.size()));
	}

}
