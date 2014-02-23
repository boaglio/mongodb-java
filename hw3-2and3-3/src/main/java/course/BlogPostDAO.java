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

package course;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

// import com.sun.org.apache.bcel.internal.generic.ACONST_NULL;

public class BlogPostDAO {

	DBCollection postsCollection;

	public BlogPostDAO(final DB blogDatabase) {
		postsCollection = blogDatabase.getCollection("posts");
	}

	// Return a single post corresponding to a permalink
	public DBObject findByPermalink(String permalink) {

		DBObject query = QueryBuilder.start("permalink").in(new String[]{permalink}).get();

		DBCursor cursor = postsCollection.find(query);
		DBObject resultElement = null;
		while (cursor.hasNext()) {
			resultElement = cursor.next();
		}
		return resultElement;

	}

	// Return a list of posts in descending order. Limit determines
	// how many posts are returned.
	public List<DBObject> findByDateDescending(int limit) {

		List<DBObject> posts = new ArrayList<DBObject>();
		// XXX HW 3.2, Work Here
		// Return a list of DBObjects, each one a post from the posts collection

		DBCursor cursor = postsCollection.find().sort(new BasicDBObject("date",-1)).limit(limit);

		while (cursor.hasNext()) {
			DBObject resultElement = cursor.next();
			posts.add(resultElement);
		}

		return posts;
	}

	public String addPost(String title,String body,List tags,String username) {

		System.out.println("inserting blog entry " + title + " " + body);

		String permalink = title.replaceAll("\\s","_"); // whitespace becomes _
		permalink = permalink.replaceAll("\\W",""); // get rid of non alphanumeric
		permalink = permalink.toLowerCase();

		BasicDBObject document = new BasicDBObject();

		// Build the post object and insert it

		document.put("title",title);
		document.put("body",body);
		document.put("permalink",permalink);
		document.put("author",username);
		document.put("tags",tags);

		Date now = new Date();
		document.put("date",now);

		postsCollection.insert(document);

		return permalink;
	}

	// White space to protect the innocent

	// Append a comment to a blog post
	public void addPostComment(final String name,String email,final String body,final String permalink) {

		// XXX HW 3.3, Work Here
		BasicDBObject newComentario = new BasicDBObject();
		newComentario.put("author",name);
		newComentario.put("body",body);
		if (email == null) {
			email = "Sem email!";
		}
		newComentario.put("email",email);

		BasicDBObject newDocument = new BasicDBObject();
		newDocument.append("$addToSet",new BasicDBObject().append("comments",newComentario));

		DBObject storedDoc = findByPermalink(permalink);

		postsCollection.update(storedDoc,newDocument);

		// Hints:
		// - email is optional and may come in NULL. Check for that.
		// - best solution uses an update command to the database and a suitable
		// operator to append the comment on to any existing list of comments

	}

}
