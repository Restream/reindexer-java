package ru.rt.restream.reindexer.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Pair<FIRST, SECOND> {
    private FIRST first;
    private SECOND second;
}