package wdm.stock.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import wdm.stock.exception.StockNotFoundException;
import wdm.stock.model.Stock;
import wdm.stock.repository.StockRepository;
import wdm.stock.service.StockService;

import java.util.Collections;
import java.util.Map;

@RestController
public class StockController {

    private final StockRepository repository;

    private final StockService stockService;

    public StockController(StockRepository repository, StockService stockService) {
        this.repository = repository;
        this.stockService = stockService;
    }


    @GetMapping("/find/{item_id}")
    Stock findStock(@PathVariable Long item_id){
        return repository.findById(item_id).orElseThrow(()-> new StockNotFoundException(item_id));
    }

    @PostMapping("/item/create/{price}")
    Map<String, Long> createItem(@PathVariable float price){
        Stock tmp = new Stock(0, price);
        repository.save(tmp);
        return Collections.singletonMap("item_id", tmp.idGet());
    }

    @PostMapping("/subtract/{item_id}/{amount}")
    @ResponseStatus(value = HttpStatus.OK)
    void subtractStock(@PathVariable Long item_id, @PathVariable int amount){
        Stock tmp = repository.findById(item_id).orElseThrow(()-> new StockNotFoundException(item_id));
        stockService.subtractStock(tmp, amount);
    }

    @PostMapping("/add/{item_id}/{amount}")
    @ResponseStatus(value = HttpStatus.OK)
    void addStock(@PathVariable Long item_id, @PathVariable int amount){
        Stock tmp = repository.findById(item_id).orElseThrow(()-> new StockNotFoundException(item_id));
        stockService.addStock(tmp, amount);
    }

    @PostMapping("/reserve/{order_id}/{item_id}/{amount}")
    @ResponseStatus(value = HttpStatus.OK)
    void reserveStock(@PathVariable Long item_id, @PathVariable Long order_id, @PathVariable int amount){
        Stock tmp = repository.findById(item_id).orElseThrow(()-> new StockNotFoundException(item_id));
        stockService.reserveStock(tmp, order_id, amount);
    }

    @PostMapping("/buy/{order_id}/{item_id}")
    @ResponseStatus(value = HttpStatus.OK)
    void buyStock(@PathVariable Long item_id, @PathVariable Long order_id){
        Stock tmp = repository.findById(item_id).orElseThrow(()-> new StockNotFoundException(item_id));
        stockService.bookStock(tmp, order_id);
    }
    
}
