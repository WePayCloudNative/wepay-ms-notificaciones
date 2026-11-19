package cl.duoc.wepay_ms_notificaciones.consumer;

import cl.duoc.wepay_ms_notificaciones.config.RabbitMQConfig;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

/**
 * Consumidor de las 3 colas principales.
 * Si un listener lanza una excepción, Spring reintenta según application.yml
 * (3 intentos con backoff) y luego rechaza el mensaje sin reencolar: RabbitMQ
 * lo envía al exchange "ex.pedidos.dlx" y queda en la DLQ correspondiente.
 */
@Component
public class Receiver {

    /** Permite simular una caída del servicio de facturación (APP_FALLA_FACTURACION=true). */
    @Value("${app.falla-facturacion:false}")
    private boolean fallaFacturacion;

    @RabbitListener(queues = RabbitMQConfig.Q_EMAIL)
    public void receiveEmail(Map<String, Object> message) throws InterruptedException {
        // Simula la latencia de un servidor SMTP (útil para ver la cola crecer en la prueba de carga)
        Thread.sleep(300);

        System.out.println("=".repeat(60));
        System.out.println("[" + timestamp() + "] [EMAIL] Nuevo correo recibido para procesar:");
        System.out.println("To: " + message.get("to"));
        System.out.println("Subject: " + message.get("subject"));
        System.out.println("Body: " + message.get("body"));
        System.out.println("=".repeat(60));
    }

    @RabbitListener(queues = RabbitMQConfig.Q_SMS)
    public void receiveSms(Map<String, Object> message) {
        System.out.println("[" + timestamp() + "] [SMS] Pedido #" + message.get("pedidoId")
                + " está " + message.get("estado") + " -> SMS enviado al cliente");
    }

    /**
     * Facturación recibe dos eventos:
     *   - ACEPTADO  (pago aprobado)          -> emite la boleta
     *   - CANCELADO                          -> emite nota de crédito solo si el pedido
     *                                           ya estaba facturado (aceptado o en preparación)
     */
    @RabbitListener(queues = RabbitMQConfig.Q_FACTURACION)
    public void receiveFacturacion(Map<String, Object> message) {
        Object pedidoId = message.get("pedidoId");
        String estado = String.valueOf(message.get("estado"));
        String estadoAnterior = String.valueOf(message.get("estadoAnterior"));

        System.out.println("[" + timestamp() + "] [FACTURACION] Procesando pedido #" + pedidoId
                + " (" + estadoAnterior + " -> " + estado + ")");
        if (fallaFacturacion) {
            throw new IllegalStateException("Servicio de facturación no disponible (simulado)");
        }

        switch (estado) {
            case "ACEPTADO" -> System.out.println("[" + timestamp() + "] [FACTURACION] Boleta generada para pedido #"
                    + pedidoId + " por $" + message.get("total"));
            case "CANCELADO" -> {
                if (FACTURADOS.contains(estadoAnterior)) {
                    System.out.println("[" + timestamp() + "] [FACTURACION] Nota de crédito generada para pedido #"
                            + pedidoId + " por $" + message.get("total") + " (anula la boleta)");
                } else {
                    System.out.println("[" + timestamp() + "] [FACTURACION] Pedido #" + pedidoId
                            + " cancelado desde " + estadoAnterior + ": no tenía boleta, no corresponde nota de crédito");
                }
            }
            default -> System.out.println("[" + timestamp() + "] [FACTURACION] Estado " + estado
                    + " ignorado para pedido #" + pedidoId);
        }
    }

    /** Estados en los que el pedido ya tiene boleta emitida. */
    private static final Set<String> FACTURADOS =
            Set.of("ACEPTADO", "EN_PREPARACION", "DESPACHADO", "ENTREGADO");

    private static String timestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
    }
}
