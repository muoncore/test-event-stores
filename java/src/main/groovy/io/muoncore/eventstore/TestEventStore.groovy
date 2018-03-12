package io.muoncore.eventstore

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import io.muoncore.Muon
import io.muoncore.liblib.reactor.rx.Streams
import io.muoncore.liblib.reactor.rx.broadcast.Broadcaster
import io.muoncore.protocol.event.Event
import io.muoncore.protocol.event.server.EventServerProtocolStack
import io.muoncore.protocol.event.server.EventWrapper
import io.muoncore.protocol.reactivestream.messages.ReactiveStreamSubscriptionRequest
import io.muoncore.protocol.reactivestream.server.PublisherLookup
import io.muoncore.protocol.reactivestream.server.ReactiveStreamServer
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

/*
 A test event store, implementing the core interfaces of Photon.
 */

@CompileStatic
class TestEventStore {

  public static Logger log = LoggerFactory.getLogger(TestEventStore)

  final List<Event> history = new ArrayList<>();
  List<Queue<Event>> subs = new ArrayList<>();
  AtomicLong orderid = new AtomicLong(1)

  void clear() {
    synchronized (history) {
      history.clear()
    }
  }

  TestEventStore(Muon muon) {

    def rs = new ReactiveStreamServer(muon)

    rs.publishGeneratedSource("/stream", PublisherLookup.PublisherType.HOT_COLD) { ReactiveStreamSubscriptionRequest request ->

      def stream = request.args["stream-name"]
      def streamType = request.args["stream-type"]
      if (!streamType) streamType = "hot-cold"

      log.info "Subscribing to $stream with type=${streamType}"

      BlockingQueue<Event> q = new LinkedBlockingQueue()
      subs << q

      if (streamType == "cold") {
        synchronized (history) {
          def items = history.findAll {
            matches(it.streamName, stream)
          }

          log.info "Requested a cold replay, found ${items.size()} items in the stream"

          if (items) {
            return Streams.from(items)
          } else {
            return Streams.empty()
          }
        }
      }

      if (request.args["stream-type"] && streamType == "hot-cold") {
        synchronized (history) {
          log.debug "Has requested hot-cold replay .. "
          history.findAll {
            matches(it.streamName, stream)
          }.each { q.add(it) }
        }
      }

      new Publisher() {
        @Override
        void subscribe(Subscriber s) {
          AtomicLong itemstoprocess = new AtomicLong(0)

          boolean running = true
          Thread t = Thread.start {
            while (running) {
              if (itemstoprocess.get() > 0) {
                def next = q.take()

                if (next.streamName == stream) {
                  log.debug("Have received data from queue, dispatching down ${stream}")
                  s.onNext(next)
                  itemstoprocess.decrementAndGet()
                }
              }
              if (itemstoprocess.get() <= 0) {
                sleep(100)
              }
            }
          }

          s.onSubscribe(new Subscription() {
            @Override
            void request(long n) {
              itemstoprocess.addAndGet(n)
            }

            @Override
            void cancel() {
              running = false
            }
          })
        }
      }
    }

    muon.protocolStacks.registerServerProtocol(new EventServerProtocolStack({ EventWrapper event ->
      log.debug "Event received " + event.event
      try {
        Event ev = event.event
        synchronized (history) {
          def orderId = orderid.addAndGet(1)
          updateEvent(orderId, ev)
          log.info("persisted $ev, will dispatch to ${subs.size()} subscribers")
          history.add(ev);
        }
        dispatchEvent(event);
        event.persisted(ev.orderId, ev.eventTime);
      } catch (Exception ex) {
        event.failed(ex.getMessage());
      }
    }, muon.getCodecs(), muon.getDiscovery()));
  }

  @CompileDynamic
  private void dispatchEvent(EventWrapper event) {
    subs.stream().forEach({ Queue<Event> q ->
      q.add(event.event)
    })
  }

  @CompileDynamic
  private void updateEvent(long orderId, Event ev) {
    ev.orderId = orderId
    ev.eventTime = System.currentTimeMillis()
  }

  def matches(name, String pattern) {
    pattern = pattern.replaceAll("\\*\\*", ".\\*")
    name ==~ pattern
  }
}

