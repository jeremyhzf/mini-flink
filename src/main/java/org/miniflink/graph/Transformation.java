package org.miniflink.graph;

/** 逻辑 DAG 节点的抽象基类。 */
public abstract class Transformation<T> {
    private final int id;
    private final String name;
    private int parallelism = 1;

    protected Transformation(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism 必须 >= 1: " + parallelism);
        }
        this.parallelism = parallelism;
    }
}
