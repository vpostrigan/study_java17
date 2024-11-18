package com.spring_boot.spring_boot.controllers;

import com.spring_boot.domain.Book;
import com.spring_boot.spring_boot.BookRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

/**
 * https://www.baeldung.com/spring-rest-openapi-documentation
 * <p>
 * http://localhost:8080  - home page
 * http://localhost:8080/api-docs/   - {"openapi":"3.0.1","info":{"title":"OpenAPI definition","version":"v0"},"servers":[{"url":"h...
 * http://localhost:8080/swagger-ui/index.html?configUrl=/api-docs/swagger-config
 */
@RestController
@RequestMapping("/api/book")
public class RestExampleController {

    @Autowired
    private BookRepository repository;

    @Operation(summary = "Get a book by its id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the book",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Book.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid id supplied",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Book not found",
                    content = @Content)})
    @GetMapping("/{id}")
    public Book findById(
            @Parameter(description = "id of book to be searched")
            @PathVariable long id) {
        Book book = new Book();
        book.setName("Name: " + id);

        repository.saveAndFlush(book);

        return repository.findById(book.getId())
                .orElseThrow(() -> new IllegalArgumentException());
    }

    @GetMapping("/")
    public Collection<Book> findBooks() {
        return repository.getBooks();
    }

    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public Book updateBook(@PathVariable("id") final String id,
                           @RequestBody final Book book) {
        return book;
    }

    @GetMapping("/filter")
    public Page<Book> filterBooks(Pageable pageable) {
        return repository.getBooks(pageable);
    }

}
