<?php

namespace Doctrine\CouchDB\HTTP;

class Tideways implements Client
{
    /**
     * @var Client
     */
    private $client;

    /**
     * Construct new logging client wrapping the real client.
     *
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    public function request($method, $path, $data = null, $raw = false, array $headers = [])
    {
        $span = null;
        $traceId = null;

        if (class_exists(\Tideways\Profiler::class)) {
            $span = \Tideways\Profiler::createSpan('http');
            $span->startTimer();

            if ($traceId = \Tideways\Profiler::currentTraceId()) {
                $headers[] = 'X-Correlation-ID: ' . $traceId;
            }
        }

        $start = microtime(true);

        $response = $this->client->request($method, $path, $data, $raw, $headers);

        if ($span) {
            $span->annotate([
                'http.url' => $path,
                'http.status' => $response->status,
                'http.method' => $method,
                'http.body' => $data,
                'http.headers' => implode("\n", $headers),
            ]);
            $span->finish();
        }

        return $response;
    }

    public function getConnection(
        $method,
        $path,
        $data = null,
        array $headers = []
    ) {
        return $this->client->getConnection($method, $path, $data, $headers);
    }

    public function getOptions()
    {
        return $this->client->getOptions();
    }

    public function setOption($option, $value)
    {
        return $this->client->setOption($option, $value);
    }
}
