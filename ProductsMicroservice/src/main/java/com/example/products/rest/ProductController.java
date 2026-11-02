package com.example.products.rest;

import java.sql.Date;

import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.products.service.ProductService;

@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductService productService;
    private final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping
    public ResponseEntity<Object> createProduct(@RequestBody CreateProductRestModel product) {

        String productId;
        try {
            productId = productService.createProduct(product);
        } catch (Exception e) {
            //e.printStackTrace();
            LOGGER.error("Error creating product: " + e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(new ErrorMessage(new Date(0), "Error creating product", e.getMessage()));
        }   

        return ResponseEntity.status(HttpStatus.CREATED).body(productId);

    }
}
