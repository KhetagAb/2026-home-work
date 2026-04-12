package company.vk.edu.distrib.compute.khetagab;

public final class EntityQueryUtils {

    private EntityQueryUtils() {
    }

    public static String parseEntityIdFromQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            throw new IllegalArgumentException("expected a non-empty query string");
        }
        for (String param : rawQuery.split("&")) {
            String[] kv = param.split("=", 2);
            if ("id".equals(kv[0])) {
                if (kv.length < 2 || kv[1].isEmpty()) {
                    throw new IllegalArgumentException("id cannot be empty");
                }
                return kv[1];
            }
        }
        throw new IllegalArgumentException("query has no id parameter");
    }
}
