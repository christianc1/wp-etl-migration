<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Loaders;

use Flow\ETL\Adapter\WordPress\Exception\{
	WPAdapterDatabaseException,
	WPAdapterDataException,
	WPAdapterMissingDataException
};
use Flow\ETL\Adapter\WordPress\Normalizers\{
    EntryNormalizer,
    RowNormalizer
};
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

/**
 * WordPress Posts Loader for ETL operations
 *
 * This loader handles the insertion and updating of WordPress posts during ETL processes.
 * It supports data normalization, sanitization, and proper error handling for post operations.
 *
 * @implements Loader
 */
final class WPPostsLoader implements Loader
{
    /**
     * @var string The format to use for datetime values
     */
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    /**
     * @var array<string, mixed> Default values for post fields
     */
    private array $postDefaults = [
        'post_status' => 'draft',
        'post_type' => 'post',
        'post_author' => 1,
    ];

    /**
     * Constructor
     *
     * @param array<string, mixed> $config Configuration array for the loader
     */
    public function __construct(
        private readonly array $config = []
    ) {
        $this->postDefaults = array_merge($this->postDefaults, $config['defaults'] ?? []);
    }

    /**
     * Creates a row normalizer instance
     *
     * @param FlowContext $context The flow context
     * @return RowNormalizer The created normalizer
     */
    public function create_normalizer(FlowContext $context): RowNormalizer
    {
        return new RowNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));
    }

    /**
     * Loads posts from the provided rows
     *
     * @param Rows $rows The rows to process
     * @param FlowContext $context The flow context
     * @throws WPAdapterMissingDataException When no posts are found to process
     * @return void
     */
    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            throw WPAdapterMissingDataException::noEntitiesFound('post', 'No posts found to process');
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($rows as $row) {
            $this->insertPost($row, $normalizer);
        }
    }

    /**
     * Inserts or updates a WordPress post
     *
     * @param Row|array<string, mixed> $row The row data to process
     * @param RowNormalizer|null $normalizer Optional normalizer for the data
     * @throws WPAdapterDatabaseException When post insertion/update fails
     * @return int The ID of the inserted/updated post
     */
    public function insertPost(Row | array $row, ?RowNormalizer $normalizer = null): int
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowNormalizer) {
            $data = $normalizer->normalize($row);
        } else {
            $data = $row;
        }

        // Sanitize input data
        $sanitizedData = $this->sanitizePostData($data);

        // Handle post ID if provided (update existing post)
        $postId = $sanitizedData['post.ID'] ?? $sanitizedData['post.id'] ?? null;

		if ( $postId ) {
			$post = get_post( $postId );

			if ( ! $post instanceof \WP_Post ) {
				$postId = null;
			}
		}

        // Prepare post data differently for updates vs new posts
        if (!empty($postId)) {
            // For updates, only include explicitly provided fields
            $postData = array_filter([
                'ID' => (int) $postId,
                'post_title' => $sanitizedData['post.post_title'] ?? $post->post_title,
                'post_content' => $sanitizedData['post.post_content'] ?? $post->post_content,
                'post_excerpt' => $sanitizedData['post.post_excerpt'] ?? $post->post_excerpt,
                'post_name' => $sanitizedData['post.post_name'] ?? $post->post_name,
                'post_status' => $sanitizedData['post.post_status'] ?? $post->post_status,
                'post_type' => $sanitizedData['post.post_type'] ?? $post->post_type,
                'post_author' => $sanitizedData['post.post_author'] ?? $post->post_author,
                'post_date' => $sanitizedData['post.post_date'] ?? $post->post_date,
                'post_date_gmt' => $sanitizedData['post.post_date_gmt'] ?? $post->post_date_gmt,
            ], function($value) { return $value !== null; });
        } else {
            // For new posts, merge with defaults
            $postData = array_merge($this->postDefaults, array_filter([
                'post_title' => $sanitizedData['post.post_title'] ?? '',
                'post_content' => $sanitizedData['post.post_content'] ?? '',
                'post_excerpt' => $sanitizedData['post.post_excerpt'] ?? '',
                'post_name' => $sanitizedData['post.post_name'] ?? '',
                'post_status' => $sanitizedData['post.post_status'] ?? $this->postDefaults['post_status'],
                'post_type' => $sanitizedData['post.post_type'] ?? $this->postDefaults['post_type'],
                'post_author' => $sanitizedData['post.post_author'] ?? $this->postDefaults['post_author'],
                'post_date' => $sanitizedData['post.post_date'] ?? current_time('mysql'),
                'post_date_gmt' => $sanitizedData['post.post_date_gmt'] ?? get_gmt_from_date($sanitizedData['post.post_date'] ?? current_time('mysql')),
            ]));
        }

        $postId = wp_insert_post($postData, true);

        if (is_wp_error($postId)) {
            throw WPAdapterDatabaseException::fromWPError($postId, "Failed to insert post: " . $postId->get_error_message());
        }

        return $postId;
    }

    /**
     * Sanitizes post data
     *
     * @param array<string, mixed> $data The post data to sanitize
     * @return array<string, mixed> Sanitized post data
     */
    private function sanitizePostData(array $data): array
    {
        $sanitized = [];

        // Sanitize text fields
        if (isset($data['post.post_title'])) {
            $sanitized['post.post_title'] = sanitize_text_field($data['post.post_title']);
        }

        if (isset($data['post.post_name'])) {
            $sanitized['post.post_name'] = sanitize_title($data['post.post_name']);
        }

        if (isset($data['post.post_excerpt'])) {
            $sanitized['post.post_excerpt'] = sanitize_text_field($data['post.post_excerpt']);
        }

        // Sanitize content with wp_kses_post to allow HTML but prevent XSS
        if (isset($data['post.post_content'])) {
            $sanitized['post.post_content'] = wp_kses_post($data['post.post_content']);
        }

        // Ensure post_type is valid
        if (isset($data['post.post_type'])) {
            $post_type = sanitize_key($data['post.post_type']);
            $sanitized['post.post_type'] = post_type_exists($post_type) ? $post_type : 'post';
        }

        // Ensure post_status is valid
        if (isset($data['post.post_status'])) {
            $status = sanitize_key($data['post.post_status']);
            $valid_statuses = get_post_stati();
            $sanitized['post.post_status'] = in_array($status, $valid_statuses) ? $status : 'draft';
        }

        // Ensure post_author is a valid user ID
        if (isset($data['post.post_author'])) {
            $author_id = absint($data['post.post_author']);
            $sanitized['post.post_author'] = get_user_by('id', $author_id) ? $author_id : 1;
        }

        // Sanitize dates
        $has_post_date = false;
        $post_date = null;

        if (isset($data['post.post_date'])) {
            // Validate date format
            $date = sanitize_text_field($data['post.post_date']);
            if ($this->validateDate($date)) {
                $sanitized['post.post_date'] = $date;
                $has_post_date = true;
                $post_date = $date;
            } else {
                $sanitized['post.post_date'] = current_time('mysql');
                $post_date = current_time('mysql');
            }
        }

        if (isset($data['post.post_date_gmt'])) {
            $date = sanitize_text_field($data['post.post_date_gmt']);
            $sanitized['post.post_date_gmt'] = $this->validateDate($date) ? $date : get_gmt_from_date(current_time('mysql'));
        } elseif ($has_post_date) {
            // If post_date is set but post_date_gmt is not, derive it from post_date
            $sanitized['post.post_date_gmt'] = get_gmt_from_date($post_date);
        }

        // Pass through IDs after sanitizing
        if (isset($data['post.ID'])) {
            $sanitized['post.ID'] = absint($data['post.ID']);
        }

        if (isset($data['post.id'])) {
            $sanitized['post.id'] = absint($data['post.id']);
        }

        // Pass through any other fields that weren't explicitly sanitized
        foreach ($data as $key => $value) {
            if (!isset($sanitized[$key])) {
                $sanitized[$key] = $value;
            }
        }

        return $sanitized;
    }

    /**
     * Validates a date string in various formats
     *
     * @param string $date Date string to validate
     * @return bool Whether the date is valid
     */
    private function validateDate(string $date): bool
    {
        // Try standard MySQL format
        $d = \DateTime::createFromFormat('Y-m-d H:i:s', $date);
        if ($d && $d->format('Y-m-d H:i:s') === $date) {
            return true;
        }

        // Try ISO 8601 format (e.g., 2017-12-31T00:00:00+00:00)
        try {
            $d = new \DateTime($date);
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * Sets the datetime format for the loader
     *
     * @param string $dateTimeFormat The format to use (should be a valid PHP date format)
     * @return self New instance with the updated format
     */
    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }

    /**
     * Sets default values for post fields
     *
     * @param array<string, mixed> $defaults The default values to set
     * @return self New instance with the updated defaults
     */
    public function withDefaults(array $defaults): self
    {
        $clone = clone $this;
        $clone->postDefaults = array_merge($this->postDefaults, $defaults);
        return $clone;
    }
}
