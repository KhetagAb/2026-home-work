package company.vk.edu.distrib.compute.khetagab.consensus;

public record ConsensusMessage(Kind kind, int senderId) {

    public enum Kind {
        PING,
        ELECT,
        ANSWER,
        VICTORY
    }
}
