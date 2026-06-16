package company.vk.edu.distrib.compute.khetagab;

import java.net.HttpURLConnection;

public final class HttpCodes {

    public static final int OK = HttpURLConnection.HTTP_OK;
    public static final int CREATED = HttpURLConnection.HTTP_CREATED;
    public static final int ACCEPTED = HttpURLConnection.HTTP_ACCEPTED;
    public static final int BAD_REQUEST = HttpURLConnection.HTTP_BAD_REQUEST;
    public static final int NOT_FOUND = HttpURLConnection.HTTP_NOT_FOUND;
    public static final int METHOD_NOT_ALLOWED = HttpURLConnection.HTTP_BAD_METHOD;
    public static final int INTERNAL_ERROR = HttpURLConnection.HTTP_INTERNAL_ERROR;

    private HttpCodes() {
    }
}
