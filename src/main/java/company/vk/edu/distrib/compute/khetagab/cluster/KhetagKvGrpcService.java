package company.vk.edu.distrib.compute.khetagab.cluster;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.khetagab.grpc.DeleteRequest;
import company.vk.edu.distrib.compute.khetagab.grpc.DeleteResponse;
import company.vk.edu.distrib.compute.khetagab.grpc.GetRequest;
import company.vk.edu.distrib.compute.khetagab.grpc.GetResponse;
import company.vk.edu.distrib.compute.khetagab.grpc.KvInternalServiceGrpc;
import company.vk.edu.distrib.compute.khetagab.grpc.PutRequest;
import company.vk.edu.distrib.compute.khetagab.grpc.PutResponse;
import io.grpc.stub.StreamObserver;

import java.util.Map;

final class KhetagKvGrpcService extends KvInternalServiceGrpc.KvInternalServiceImplBase {

    private final Map<String, byte[]> storage;
    private final Map<String, Boolean> tombstones;

    KhetagKvGrpcService(Map<String, byte[]> storage, Map<String, Boolean> tombstones) {
        super();
        this.storage = storage;
        this.tombstones = tombstones;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        String key = request.getKey();
        GetResponse.Builder builder = GetResponse.newBuilder();
        if (!tombstones.containsKey(key)) {
            byte[] val = storage.get(key);
            if (val != null) {
                builder.setFound(true).setValue(ByteString.copyFrom(val));
            }
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String key = request.getKey();
        byte[] value = request.getValue().toByteArray();
        tombstones.remove(key);
        storage.put(key, value);
        responseObserver.onNext(PutResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String key = request.getKey();
        storage.remove(key);
        tombstones.put(key, Boolean.TRUE);
        responseObserver.onNext(DeleteResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
