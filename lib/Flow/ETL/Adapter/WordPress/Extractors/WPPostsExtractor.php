<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Extractors;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor\{Limitable, LimitableExtractor};
use Flow\ETL\{Extractor, FlowContext, Row, Rows};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\Signal;

final class WPPostsExtractor implements Extractor, LimitableExtractor
{
    use Limitable;

    private array $postDefaults = [
        'post_type' => 'post',
        'post_status' => ['publish', 'draft', 'private'],
        'posts_per_page' => 10,
        'orderby' => 'ID',
        'order' => 'ASC',
    ];

    private bool $includeMeta = true;
    private bool $includeTaxonomies = false;
    private bool $expandAuthorData = false;
    private ?array $taxonomies = null;

    public function __construct(
        private readonly array $config = []
    ) {
        $this->postDefaults = array_merge($this->postDefaults, $config['defaults'] ?? []);
        $this->resetLimit();
    }

    public function extract(FlowContext $context): \Generator
    {
        $page = 1;
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        do {
            $args = array_merge($this->postDefaults, [
                'paged' => $page,
                'fields' => 'all',
                'suppress_filters' => false,
            ]);

            $query = new \WP_Query($args);

            if ($query->have_posts()) {
                while ($query->have_posts()) {
                    $query->the_post();
                    $post = $query->post;

                    $postData = $this->normalizePost($post);

                    if ($this->includeMeta) {
                        $postData['meta'] = get_post_meta($post->ID);
                    }

                    if ($this->includeTaxonomies) {
                        $postData['taxonomies'] = $this->getPostTaxonomies($post);
                    }

                    if ($this->expandAuthorData) {
                        $postData['author'] = $this->getAuthorData((int) $post->post_author);
                    }

                    if ($shouldPutInputIntoRows) {
                        $postData['_input_source'] = 'wp_posts';
                    }

                    $signal = yield array_to_rows($postData, $context->entryFactory());
                    $this->incrementReturnedRows();

                    if ($signal === Signal::STOP || $this->reachedLimit()) {
                        wp_reset_postdata();
                        return;
                    }
                }
                $page++;
            } else {
                break;
            }

            wp_reset_postdata();
        } while ($query->max_num_pages >= $page);
    }

    private function normalizePost(\WP_Post $post): array
    {
        return [
            'ID' => (int) $post->ID,
            'post_author' => (int) $post->post_author,
            'post_date' => $post->post_date,
            'post_date_gmt' => $post->post_date_gmt,
            'post_content' => $post->post_content,
            'post_title' => $post->post_title,
            'post_excerpt' => $post->post_excerpt,
            'post_status' => $post->post_status,
            'comment_status' => $post->comment_status,
            'ping_status' => $post->ping_status,
            'post_password' => $post->post_password,
            'post_name' => $post->post_name,
            'to_ping' => $post->to_ping,
            'pinged' => $post->pinged,
            'post_modified' => $post->post_modified,
            'post_modified_gmt' => $post->post_modified_gmt,
            'post_content_filtered' => $post->post_content_filtered,
            'post_parent' => (int) $post->post_parent,
            'guid' => $post->guid,
            'menu_order' => (int) $post->menu_order,
            'post_type' => $post->post_type,
            'post_mime_type' => $post->post_mime_type,
            'comment_count' => (int) $post->comment_count,
        ];
    }

    private function getPostTaxonomies(\WP_Post $post): array
    {
        $taxonomies = $this->taxonomies ?? get_object_taxonomies($post->post_type);
        $terms = [];

        foreach ($taxonomies as $taxonomy) {
            $postTerms = wp_get_post_terms($post->ID, $taxonomy, ['fields' => 'all']);
            if (!is_wp_error($postTerms)) {
                $terms[$taxonomy] = array_map(function($term) {
                    return [
                        'term_id' => (int) $term->term_id,
                        'name' => $term->name,
                        'slug' => $term->slug,
                        'term_group' => (int) $term->term_group,
                        'term_taxonomy_id' => (int) $term->term_taxonomy_id,
                        'taxonomy' => $term->taxonomy,
                        'description' => $term->description,
                        'parent' => (int) $term->parent,
                        'count' => (int) $term->count,
                    ];
                }, $postTerms);
            }
        }

        return $terms;
    }

    private function getAuthorData(int $authorId): array
    {
        $author = get_userdata($authorId);
        if (!$author) {
            return [];
        }

        return [
            'ID' => (int) $author->ID,
            'user_login' => $author->user_login,
            'user_nicename' => $author->user_nicename,
            'user_email' => $author->user_email,
            'user_url' => $author->user_url,
            'display_name' => $author->display_name,
            'first_name' => $author->first_name,
            'last_name' => $author->last_name,
            'nickname' => $author->nickname,
            'description' => $author->description,
            'roles' => $author->roles,
        ];
    }

    public function withPostType(string $postType): self
    {
        $clone = clone $this;
        $clone->postDefaults['post_type'] = $postType;
        return $clone;
    }

    public function withPostStatus(array|string $postStatus): self
    {
        $clone = clone $this;
        $clone->postDefaults['post_status'] = $postStatus;
        return $clone;
    }

    public function withPostsPerPage(int $postsPerPage): self
    {
        $clone = clone $this;
        $clone->postDefaults['posts_per_page'] = $postsPerPage;
        return $clone;
    }

    public function withOrderBy(string $orderBy): self
    {
        $clone = clone $this;
        $clone->postDefaults['orderby'] = $orderBy;
        return $clone;
    }

    public function withOrder(string $order): self
    {
        $clone = clone $this;
        $clone->postDefaults['order'] = strtoupper($order);
        return $clone;
    }

    public function withMeta(bool $includeMeta = true): self
    {
        $clone = clone $this;
        $clone->includeMeta = $includeMeta;
        return $clone;
    }

    public function withTaxonomies(array|bool $taxonomies = true): self
    {
        $clone = clone $this;
        if (is_bool($taxonomies)) {
            $clone->includeTaxonomies = $taxonomies;
            $clone->taxonomies = null;
        } else {
            $clone->includeTaxonomies = true;
            $clone->taxonomies = $taxonomies;
        }
        return $clone;
    }

    public function withExpandedAuthorData(bool $expandAuthorData = true): self
    {
        $clone = clone $this;
        $clone->expandAuthorData = $expandAuthorData;
        return $clone;
    }
}
