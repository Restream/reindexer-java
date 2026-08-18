/*
 * Copyright 2020 Restream
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
package ru.rt.restream.reindexer.connector;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.jupiter.api.Test;
import ru.rt.restream.reindexer.NamespaceOptions;
import ru.rt.restream.reindexer.Query;
import ru.rt.restream.reindexer.annotations.Reindex;
import ru.rt.restream.reindexer.annotations.Transient;
import ru.rt.restream.reindexer.db.DbBaseTest;
import ru.rt.restream.reindexer.exceptions.ReindexerException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.rt.restream.reindexer.Query.Condition.EQ;
import static ru.rt.restream.reindexer.Query.Condition.SET;

/**
 * Base nested join test.
 */
public abstract class NestedJoinTest extends DbBaseTest {

    private static final String BOOKS_NS = "nested_join_books";
    private static final String AUTHORS_NS = "nested_join_authors";
    private static final String LOCATIONS_NS = "nested_join_locations";
    private static final String COUNTRIES_NS = "nested_join_countries";

    @Test
    public void testNestedInnerJoin() {
        openNamespaces();
        insertFixture();

        Query<Author> authors = db.query(AUTHORS_NS, Author.class)
                .innerJoin(db.query(LOCATIONS_NS, Location.class)
                        .on("locationId", EQ, "id"), "locations")
                .on("authorId", EQ, "id");

        Map<Integer, Book> booksById = byId(db.query(BOOKS_NS, Book.class)
                .innerJoin(authors, "authors")
                .toList());

        assertThat(booksById.size(), is(3));
        assertAuthorLocation(booksById.get(1000), 100, "Author1", "Moscow");
        assertAuthorLocation(booksById.get(1002), 100, "Author1", "Moscow");
        assertAuthorLocation(booksById.get(1003), 101, "Author2", "Paris");
        assertThat(booksById.containsKey(1001), is(false));
        assertThat(booksById.containsKey(1004), is(false));
    }

    @Test
    public void testLeftJoinWithNestedInnerJoin() {
        openNamespaces();
        insertFixture();

        Query<Author> authors = db.query(AUTHORS_NS, Author.class)
                .innerJoin(db.query(LOCATIONS_NS, Location.class)
                        .on("locationId", EQ, "id"), "locations")
                .on("authorId", EQ, "id");

        Map<Integer, Book> booksById = byId(db.query(BOOKS_NS, Book.class)
                .leftJoin(authors, "authors")
                .toList());

        assertThat(booksById.size(), is(5));
        assertAuthorLocation(booksById.get(1000), 100, "Author1", "Moscow");
        assertThat(booksById.get(1001).authors.size(), is(0));
        assertAuthorLocation(booksById.get(1002), 100, "Author1", "Moscow");
        assertAuthorLocation(booksById.get(1003), 101, "Author2", "Paris");
        assertThat(booksById.get(1004).authors.size(), is(0));
    }

    @Test
    public void testInnerJoinWithNestedEmptyLeftJoin() {
        openNamespaces();
        insertFixture();

        Query<Author> authors = db.query(AUTHORS_NS, Author.class)
                .leftJoin(db.query(LOCATIONS_NS, Location.class)
                        .where("city", EQ, "NoSuchCity")
                        .on("locationId", EQ, "id"), "locations")
                .on("authorId", EQ, "id");

        List<Book> books = db.query(BOOKS_NS, Book.class)
                .where("title", EQ, "Book1")
                .innerJoin(authors, "authors")
                .toList();

        assertThat(books.size(), is(1));
        Book book = books.get(0);
        assertThat(book.authors.size(), is(1));
        Author author = book.authors.get(0);
        assertThat(author.id, is(book.authorId));
        assertThat(author.name, is("Author1"));
        assertThat(author.locations.size(), is(0));
    }

    @Test
    public void testMergeWithNestedJoins() {
        openNamespaces();
        insertFixture();

        Query<Book> first = db.query(BOOKS_NS, Book.class)
                .where("title", EQ, "Book1")
                .innerJoin(db.query(AUTHORS_NS, Author.class)
                        .innerJoin(db.query(LOCATIONS_NS, Location.class)
                                .on("locationId", EQ, "id"), "locations")
                        .on("authorId", EQ, "id"), "authors");

        Query<Book> second = db.query(BOOKS_NS, Book.class)
                .where("title", EQ, "OtherBook")
                .innerJoin(db.query(AUTHORS_NS, Author.class)
                        .innerJoin(db.query(LOCATIONS_NS, Location.class)
                                .on("locationId", EQ, "id"), "locations")
                        .on("authorId", EQ, "id"), "authors");

        List<Book> books = first.merge(second).toList();
        assertThat(books.size(), is(2));

        Map<Integer, Book> booksById = byId(books);
        assertAuthorLocation(booksById.get(1000), 100, "Author1", "Moscow");
        assertAuthorLocation(booksById.get(1002), 100, "Author1", "Moscow");
    }

    @Test
    public void testNestedJoinDepthTwoPlus() {
        openNamespaces();
        insertFixture();

        Query<Location> locations = db.query(LOCATIONS_NS, Location.class)
                .innerJoin(db.query(COUNTRIES_NS, Country.class)
                        .on("countryId", EQ, "id"), "countries")
                .on("locationId", EQ, "id");

        Query<Author> authors = db.query(AUTHORS_NS, Author.class)
                .innerJoin(locations, "locations")
                .on("authorId", EQ, "id");

        Map<Integer, Book> booksById = byId(db.query(BOOKS_NS, Book.class)
                .innerJoin(authors, "authors")
                .toList());

        assertThat(booksById.size(), is(3));
        assertAuthorLocation(booksById.get(1000), 100, "Author1", "Moscow");
        assertThat(booksById.get(1000).authors.get(0).locations.get(0).countries.get(0).name, is("Northern"));
        assertAuthorLocation(booksById.get(1002), 100, "Author1", "Moscow");
        assertAuthorLocation(booksById.get(1003), 101, "Author2", "Paris");
        assertThat(booksById.get(1003).authors.get(0).locations.get(0).countries.get(0).name, is("Southern"));
    }

    @Test
    public void testCannotJoinWithMerge() {
        openNamespaces();
        insertFixture();

        Query<Author> authors = db.query(AUTHORS_NS, Author.class)
                .merge(db.query(AUTHORS_NS, Author.class))
                .on("authorId", EQ, "id");

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> db.query(BOOKS_NS, Book.class)
                        .innerJoin(authors, "authors")
                        .execute());

        assertThat(exception.getMessage(), containsString("MERGEs nested into the JOINs are not supported"));
    }

    @Test
    public void testCannotUseJoinInSubquery() {
        openNamespaces();
        insertFixture();

        Query<Author> subQuery = db.query(AUTHORS_NS, Author.class)
                .select("id")
                .innerJoin(db.query(LOCATIONS_NS, Location.class)
                        .on("locationId", EQ, "id"), "locations");

        ReindexerException exception = assertThrows(ReindexerException.class,
                () -> db.query(BOOKS_NS, Book.class)
                        .where("authorId", SET, subQuery)
                        .execute());

        assertThat(exception.getMessage(), containsString("Join cannot be in subquery"));
    }

    @Test
    public void testCannotUseMergeInSubquery() {
        openNamespaces();
        insertFixture();

        Query<Author> subQuery = db.query(AUTHORS_NS, Author.class)
                .select("id")
                .merge(db.query(AUTHORS_NS, Author.class).select("id"));

        ReindexerException exception = assertThrows(ReindexerException.class,
                () -> db.query(BOOKS_NS, Book.class)
                        .where("authorId", SET, subQuery)
                        .execute());

        assertThat(exception.getMessage(), containsString("Merge cannot be in subquery"));
    }

    private void openNamespaces() {
        db.openNamespace(BOOKS_NS, NamespaceOptions.defaultOptions(), Book.class);
        db.openNamespace(AUTHORS_NS, NamespaceOptions.defaultOptions(), Author.class);
        db.openNamespace(LOCATIONS_NS, NamespaceOptions.defaultOptions(), Location.class);
        db.openNamespace(COUNTRIES_NS, NamespaceOptions.defaultOptions(), Country.class);
    }

    private void insertFixture() {
        db.upsert(COUNTRIES_NS, new Country(1, "Northern"));
        db.upsert(COUNTRIES_NS, new Country(2, "Southern"));
        db.upsert(LOCATIONS_NS, new Location(10, "Moscow", 1));
        db.upsert(LOCATIONS_NS, new Location(20, "Paris", 2));
        db.upsert(AUTHORS_NS, new Author(100, "Author1", 10));
        db.upsert(AUTHORS_NS, new Author(101, "Author2", 20));
        db.upsert(AUTHORS_NS, new Author(102, "AuthorNoLoc", 999));
        db.upsert(BOOKS_NS, new Book(1000, "Book1", 100));
        db.upsert(BOOKS_NS, new Book(1001, "Book2", 999));
        db.upsert(BOOKS_NS, new Book(1002, "OtherBook", 100));
        db.upsert(BOOKS_NS, new Book(1003, "Book3", 101));
        db.upsert(BOOKS_NS, new Book(1004, "BookNoLoc", 102));
    }

    private static void assertAuthorLocation(Book book, int expectedAuthorId, String expectedAuthorName,
                                             String expectedCity) {
        assertThat(book.authors.size(), is(1));
        Author author = book.authors.get(0);
        assertThat(author.id, is(expectedAuthorId));
        assertThat(author.id, is(book.authorId));
        assertThat(author.name, is(expectedAuthorName));
        assertThat(author.locations.size(), is(1));
        assertThat(author.locations.get(0).id, is(author.locationId));
        assertThat(author.locations.get(0).city, is(expectedCity));
    }

    private static Map<Integer, Book> byId(List<Book> books) {
        Map<Integer, Book> booksById = new HashMap<>();
        for (Book book : books) {
            booksById.put(book.id, book);
        }
        return booksById;
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class Book {
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;

        @Reindex(name = "title")
        private String title;

        @Reindex(name = "authorId")
        private int authorId;

        @Transient
        private List<Author> authors;

        public Book(int id, String title, int authorId) {
            this.id = id;
            this.title = title;
            this.authorId = authorId;
        }
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class Author {
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;

        @Reindex(name = "name")
        private String name;

        @Reindex(name = "locationId")
        private int locationId;

        @Transient
        private List<Location> locations;

        public Author(int id, String name, int locationId) {
            this.id = id;
            this.name = name;
            this.locationId = locationId;
        }
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class Location {
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;

        @Reindex(name = "city")
        private String city;

        @Reindex(name = "countryId")
        private int countryId;

        @Transient
        private List<Country> countries;

        public Location(int id, String city, int countryId) {
            this.id = id;
            this.city = city;
            this.countryId = countryId;
        }
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class Country {
        @Reindex(name = "id", isPrimaryKey = true)
        private int id;

        @Reindex(name = "name")
        private String name;

        public Country(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

}
