package org.sagebionetworks.table.query;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sagebionetworks.table.query.model.Pagination;

public class PaginationTest {
	
	@Test
	public void testToSQL(){
		Pagination element = new Pagination("123", "456");
		StringBuilder builder = new StringBuilder();
		element.toSQL(builder);
		assertEquals("LIMIT 123 OFFSET 456", builder.toString());
	}

}
