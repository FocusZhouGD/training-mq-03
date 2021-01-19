package io.github.huobidev.qinjinwei;

import io.github.huobidev.Order;

public interface Producer {


    void send(Order order);

    void close();

}
