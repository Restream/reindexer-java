/*
 * Copyright 2020-present Restream
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.rt.restream.reindexer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static ru.rt.restream.reindexer.binding.Consts.INNER_JOIN;
import static ru.rt.restream.reindexer.binding.Consts.LEFT_JOIN;

@Tag("builtin")
@Tag("cproto")
public class QueryLogBuilderTest {

    private static final int OP_AND = 2;
    private static final int EQ = 1;

    @Test
    public void testFlatLeftJoinStaysUnwrapped() {
        QueryLogBuilder books = query("books");
        join(books, query("authors"), LEFT_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books LEFT JOIN authors ON authors.id = books.authorId"));
    }

    @Test
    public void testFlatInnerJoinStaysUnwrapped() {
        QueryLogBuilder books = query("books");
        join(books, query("authors"), INNER_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books WHERE INNER JOIN authors ON authors.id = books.authorId"));
    }

    @Test
    public void testNestedLeftJoinIsDumped() {
        QueryLogBuilder authors = query("authors");
        join(authors, query("locations"), LEFT_JOIN, "locationId", "id");

        QueryLogBuilder books = query("books");
        join(books, authors, LEFT_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books LEFT JOIN (SELECT * FROM authors "
                        + "LEFT JOIN locations ON locations.id = authors.locationId) "
                        + "ON authors.id = books.authorId"));
    }

    @Test
    public void testInnerJoinWithNestedLeftJoinIsDumped() {
        QueryLogBuilder authors = query("authors");
        join(authors, query("locations"), LEFT_JOIN, "locationId", "id");

        QueryLogBuilder books = query("books");
        join(books, authors, INNER_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books WHERE INNER JOIN (SELECT * FROM authors "
                        + "LEFT JOIN locations ON locations.id = authors.locationId) "
                        + "ON authors.id = books.authorId"));
    }

    @Test
    public void testNestedInnerJoinIsDumpedViaWhere() {
        QueryLogBuilder authors = query("authors");
        join(authors, query("locations"), INNER_JOIN, "locationId", "id");

        QueryLogBuilder books = query("books");
        join(books, authors, INNER_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books WHERE INNER JOIN (SELECT * FROM authors "
                        + "WHERE INNER JOIN locations ON locations.id = authors.locationId) "
                        + "ON authors.id = books.authorId"));
    }

    @Test
    public void testNestedJoinDepthTwoPlus() {
        QueryLogBuilder locations = query("locations");
        join(locations, query("countries"), INNER_JOIN, "countryId", "id");

        QueryLogBuilder authors = query("authors");
        join(authors, locations, INNER_JOIN, "locationId", "id");

        QueryLogBuilder books = query("books");
        join(books, authors, INNER_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books WHERE INNER JOIN (SELECT * FROM authors "
                        + "WHERE INNER JOIN (SELECT * FROM locations "
                        + "WHERE INNER JOIN countries ON countries.id = locations.countryId) "
                        + "ON locations.id = authors.locationId) "
                        + "ON authors.id = books.authorId"));
    }

    @Test
    public void testNestedLeftJoinDepthTwoPlus() {
        QueryLogBuilder locations = query("locations");
        join(locations, query("countries"), LEFT_JOIN, "countryId", "id");

        QueryLogBuilder authors = query("authors");
        join(authors, locations, LEFT_JOIN, "locationId", "id");

        QueryLogBuilder books = query("books");
        join(books, authors, LEFT_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books LEFT JOIN (SELECT * FROM authors "
                        + "LEFT JOIN (SELECT * FROM locations "
                        + "LEFT JOIN countries ON countries.id = locations.countryId) "
                        + "ON locations.id = authors.locationId) "
                        + "ON authors.id = books.authorId"));
    }

    @Test
    public void testJoinInsideMerge() {
        QueryLogBuilder firstAuthors = query("authors");
        join(firstAuthors, query("locations"), LEFT_JOIN, "locationId", "id");
        QueryLogBuilder first = query("books");
        first.where(OP_AND, "title", EQ, "Book1");
        join(first, firstAuthors, INNER_JOIN, "authorId", "id");

        QueryLogBuilder secondAuthors = query("authors");
        join(secondAuthors, query("locations"), LEFT_JOIN, "locationId", "id");
        QueryLogBuilder second = query("books");
        second.where(OP_AND, "title", EQ, "OtherBook");
        join(second, secondAuthors, INNER_JOIN, "authorId", "id");

        first.merge(second);

        assertThat(first.getSql(), is(
                "SELECT * FROM books WHERE title = 'Book1' AND INNER JOIN (SELECT * FROM authors "
                        + "LEFT JOIN locations ON locations.id = authors.locationId) "
                        + "ON authors.id = books.authorId MERGE(SELECT * FROM books WHERE title = 'OtherBook' "
                        + "AND INNER JOIN (SELECT * FROM authors "
                        + "LEFT JOIN locations ON locations.id = authors.locationId) "
                        + "ON authors.id = books.authorId)"));
    }

    @Test
    public void testJoinedQueryWithLimitIsDumped() {
        QueryLogBuilder authors = query("authors");
        authors.limit(10);
        QueryLogBuilder books = query("books");
        join(books, authors, LEFT_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books LEFT JOIN (SELECT * FROM authors LIMIT 10) ON authors.id = books.authorId"));
    }

    @Test
    public void testJoinedQueryWithOffsetIsDumped() {
        QueryLogBuilder authors = query("authors");
        authors.offset(5);
        QueryLogBuilder books = query("books");
        join(books, authors, LEFT_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books LEFT JOIN (SELECT * FROM authors OFFSET 5) ON authors.id = books.authorId"));
    }

    @Test
    public void testJoinedQueryWithSelectIsDumped() {
        QueryLogBuilder authors = query("authors");
        authors.select("id");
        QueryLogBuilder books = query("books");
        join(books, authors, LEFT_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books LEFT JOIN (SELECT id FROM authors) ON authors.id = books.authorId"));
    }

    @Test
    public void testJoinedQueryWithSortIsDumped() {
        QueryLogBuilder authors = query("authors");
        authors.sort("name", false);
        QueryLogBuilder books = query("books");
        join(books, authors, LEFT_JOIN, "authorId", "id");

        assertThat(books.getSql(), is(
                "SELECT * FROM books LEFT JOIN (SELECT * FROM authors ORDER BY 'name') "
                        + "ON authors.id = books.authorId"));
    }

    private static QueryLogBuilder query(String namespace) {
        QueryLogBuilder builder = new QueryLogBuilder();
        builder.namespace(namespace);
        return builder;
    }

    private static void join(QueryLogBuilder parent, QueryLogBuilder joined, int joinType,
                             String parentField, String joinIndex) {
        joined.on(OP_AND, parentField, EQ, joinIndex);
        parent.join(joined, joinType);
    }

}
