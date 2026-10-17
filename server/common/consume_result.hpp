#pragma once

namespace bite_im {

// Only Ack permits removing a successfully processed delivery from the queue.
enum class ConsumeResult {
    Ack,
    Retry,
    Reject
};

} // namespace bite_im
