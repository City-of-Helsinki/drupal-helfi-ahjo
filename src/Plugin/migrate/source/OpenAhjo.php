<?php

declare(strict_types = 1);

namespace Drupal\helfi_ahjo\Plugin\migrate\source;

use Drupal\Component\Utility\UrlHelper;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Url;
use Drupal\helfi_api_base\MigrateTrait;
use Drupal\migrate\Plugin\migrate\source\SourcePluginBase;
use Drupal\migrate\Plugin\MigrationInterface;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\GuzzleException;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Source plugin for retrieving data from OpenAhjo.
 *
 * @MigrateSource(
 *   id = "open_ahjo"
 * )
 */
class OpenAhjo extends SourcePluginBase implements ContainerFactoryPluginInterface {

  use MigrateTrait;

  /**
   * The number of ignored rows until we stop the migrate.
   *
   * This assumes that your API can be sorted in a way that the newest
   * changes are listed first.
   *
   * For this to have any effect 'track_changes' source setting must be set to
   * true and you must run the migrate with PARTIAL_MIGRATE=1 setting.
   *
   * @var int
   */
  protected const NUM_IGNORED_ROWS_BEFORE_STOPPING = 20;

  /**
   * The http client.
   *
   * @var \GuzzleHttp\ClientInterface
   */
  protected ClientInterface $httpClient;

  /**
   * The entity count.
   *
   * @var int
   */
  protected int $count = 0;

  /**
   * An array of urls to fetch.
   *
   * @var string[]
   */
  protected array $urls = [];

  /**
   * The limit per page.
   *
   * @var int
   */
  protected int $limit = 0;

  /**
   * Keep track of ignored rows to stop migrate after N ignored rows.
   *
   * @var int
   */
  protected int $ignoredRows = 0;

  /**
   * {@inheritdoc}
   */
  public function __toString() {
    return 'OpenAhjo';
  }

  /**
   * {@inheritdoc}
   */
  public function getIds() {
    return ['id' => ['type' => 'string']];
  }

  /**
   * {@inheritdoc}
   */
  public function fields() {
    return [];
  }

  /**
   * {@inheritdoc}
   */
  public function count($refresh = FALSE) {
    if (!$this->count) {
      $this->count = $this->doCount();
    }
    return $this->count;
  }

  /**
   * {@inheritdoc}
   */
  protected function doCount() : int {
    $source_data = $this->getContent($this->configuration['url']);

    foreach (['limit', 'offset', 'total_count'] as $key) {
      if (!isset($source_data['meta'][$key])) {
        throw new \InvalidArgumentException(sprintf('The "%s" value is missing from meta[].', $key));
      }
    }
    ['limit' => $this->limit, 'total_count' => $count] = $source_data['meta'];

    $totalPages = ceil($count / $this->limit);

    // Limit total pages to N if configured so.
    if (isset($this->configuration['limit_pages'])) {
      $limitPages = (int) $this->configuration['limit_pages'];

      $totalPages = ($totalPages > $limitPages) ? $limitPages : $totalPages;
      $count = ($totalPages * $this->limit);
    }
    return $count;
  }

  /**
   * Builds the metadata.
   */
  protected function buildUrls() : self {
    $this->count();
    $currentUrl = UrlHelper::parse($this->configuration['url']);

    for ($i = 0; $i < ($this->count / $this->limit); $i++) {
      $currentUrl['query']['offset'] = $this->limit * $i;

      $this->urls[] = Url::fromUri($currentUrl['path'], [
        'query' => $currentUrl['query'],
        'fragment' => $currentUrl['fragment'],
      ])->toString();
    }

    return $this;
  }

  /**
   * Gets the remote data.
   *
   * @return \Generator
   *   The remote data.
   */
  protected function getRemoteData() : \Generator {
    $processed = 0;

    foreach ($this->urls as $url) {
      $content = $this->getContent($url);

      foreach ($content['objects'] as $object) {
        // Skip entire migration once we've reached the number of maximum
        // ignored (not changed) rows.
        // @see static::NUM_IGNORED_ROWS_BEFORE_STOPPING.
        if ($this->isPartialMigrate() && ($this->ignoredRows >= static::NUM_IGNORED_ROWS_BEFORE_STOPPING)) {
          break 2;
        }
        $processed++;

        // Allow number of items to be limited by using an env variable.
        if (($this->getLimit() > 0) && $processed > $this->getLimit()) {
          break 2;
        }
        yield $object;
      }
    }
  }

  /**
   * {@inheritdoc}
   */
  public function next() {
    parent::next();

    // Check if the current row has changes and increment ignoredRows variable
    // to allow us to stop migrate early if we have no changes.
    if ($this->isPartialMigrate() && $this->currentRow && !$this->currentRow->changed()) {
      $this->ignoredRows++;
    }
  }

  /**
   * Sends a HTTP request and returns response data as array.
   *
   * @param string $url
   *   The url.
   *
   * @return array
   *   The JSON returned by Ahjo service.
   */
  protected function getContent(string $url) : array {
    try {
      $content = (string) $this->httpClient->request('GET', $url)->getBody();
    }
    catch (GuzzleException $e) {
      return [];
    }
    return \GuzzleHttp\json_decode($content, TRUE);
  }

  /**
   * {@inheritdoc}
   */
  protected function initializeIterator() {
    $this->buildUrls();

    return $this->getRemoteData();
  }

  /**
   * {@inheritdoc}
   */
  public static function create(
    ContainerInterface $container,
    array $configuration,
    $plugin_id,
    $plugin_definition,
    MigrationInterface $migration = NULL
  ) {
    $instance = new static($configuration, $plugin_id, $plugin_definition, $migration);
    $instance->httpClient = $container->get('http_client');

    if (!isset($configuration['url'])) {
      throw new \InvalidArgumentException('The "url" configuration missing.');
    }
    return $instance;
  }

}
